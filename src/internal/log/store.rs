use std::fs::{File, Metadata};
use std::io::{self, BufWriter, Seek, SeekFrom};
use std::io::{Read, Write};
// use std::os::unix::fs::FileExt;
use byteorder::BigEndian;
use byteorder::WriteBytesExt;
use std::sync::Mutex;

const LEN_WIDTH: usize = 8;

struct Store {
    file: File,
    buf: Mutex<BufWriter<File>>,
    size: u64,
}

impl Store {
    fn new_store(file: File) -> io::Result<Store> {
        let metadata: Metadata = file.metadata()?;
        let size: u64 = metadata.len();
        let new_file = file.try_clone()?;

        Ok(Store {
            file,
            buf: Mutex::new(BufWriter::new(new_file)),
            size,
        })
    }

    fn append(&mut self, p: &[u8]) -> Result<(u64, u64), Box<dyn std::error::Error + '_>> {
        let mut buf = self
            .buf
            .lock()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        let pos = self.size;
        buf.write_u64::<BigEndian>(p.len() as u64)?;
        let w = buf.write(p)?;
        let total_written = w + LEN_WIDTH;
        self.size += total_written as u64;
        buf.flush()?;
        Ok((total_written as u64, pos))
    }

    fn read(&mut self, pos: u64) -> Result<Vec<u8>, Box<dyn std::error::Error + '_>> {
        let mut buf = self
            .buf
            .lock()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        let mut size_buf = [0u8; LEN_WIDTH];
        buf.seek(SeekFrom::Start(pos))?;
        self.file.read_exact(&mut size_buf)?;
        let len = u64::from_be_bytes(size_buf);

        let mut data = vec![0u8; len as usize];
        buf.seek(SeekFrom::Start(pos + LEN_WIDTH as u64))?;
        self.file.read_exact(&mut data)?;

        Ok(data)
    }

    fn read_at(&mut self, p: &mut [u8], off: u64) -> io::Result<usize> {
        let mut buf = self
            .buf
            .lock()
            .map_err(|e| io::Error::other(e.to_string()))?;
        buf.flush()?;
        self.file.seek(SeekFrom::Start(off))?;
        let bytes_read = self.file.read(p)?;

        Ok(bytes_read)
    }

    fn close(&mut self) -> io::Result<()> {
        let mut buf = self
            .buf
            .lock()
            .map_err(|e| io::Error::other(e.to_string()))?;
        buf.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Store, LEN_WIDTH};
    use std::fs::File;
    use std::fs::OpenOptions;
    use std::io::{self};
    use tempfile::NamedTempFile;

    const WRITE: &[u8] = b"hello world";
    const WIDTH: u64 = (WRITE.len() as u64) + LEN_WIDTH as u64;

    #[test]
    fn test_store_append_read() -> io::Result<()> {
        let temp_file = NamedTempFile::new()?;
        let file_path = temp_file.path().to_path_buf();

        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&file_path)?;
        let mut store = Store::new_store(file)?;

        test_append(&mut store)?;
        test_read(&mut store)?;
        test_read_at(&mut store)?;

        let file = File::open(&file_path)?;
        let mut store = Store::new_store(file)?;
        test_read(&mut store)?;

        Ok(())
    }

    #[test]
    fn test_store_close() -> io::Result<()> {
        let temp_file = NamedTempFile::new()?;
        let file_path = temp_file.path().to_path_buf();

        // Create a store
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&file_path)?;
        let mut store = Store::new_store(file)?;

        let (_, _) = store
            .append(WRITE)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let (before_file, before_size) = open_file(&file_path)?;

        store.close()?;

        let (after_file, after_size) = open_file(&file_path)?;

        assert!(
            after_size == before_size,
            "expected after_size {} > before_size {}",
            after_size,
            before_size
        );

        drop(before_file);
        drop(after_file);

        Ok(())
    }

    fn open_file(name: &std::path::Path) -> io::Result<(File, i64)> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(name)?;
        let metadata = file.metadata()?;
        let size = metadata.len() as i64;
        Ok((file, size))
    }

    fn test_append(store: &mut Store) -> io::Result<()> {
        let (n, pos) = store
            .append(WRITE)
            .map_err(|e| io::Error::other(e.to_string()))?;
        assert_eq!(n, WIDTH, "expected {} bytes written, got {}", WIDTH, n);
        assert_eq!(pos, 0, "expected position 0, got {}", pos);
        Ok(())
    }

    fn test_read(store: &mut Store) -> io::Result<()> {
        let data = store.read(0).map_err(|e| io::Error::other(e.to_string()))?;
        assert_eq!(data, WRITE, "expected read data to match {:?}", WRITE);
        Ok(())
    }

    fn test_read_at(store: &mut Store) -> io::Result<()> {
        let mut buffer = vec![0u8; WRITE.len()];
        let bytes_read = store.read_at(&mut buffer, LEN_WIDTH as u64)?;
        assert_eq!(
            bytes_read,
            WRITE.len(),
            "expected {} bytes read, got {}",
            WRITE.len(),
            bytes_read
        );
        assert_eq!(buffer, WRITE, "expected read_at data to match {:?}", WRITE);
        Ok(())
    }
}

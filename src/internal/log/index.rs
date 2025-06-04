use crate::internal::log::config::Config;
use byteorder::{BigEndian, ByteOrder};
use memmap2::{MmapMut, MmapOptions};
use std::fs::File;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

#[cfg(target_os = "linux")]
fn get_file_path(file: &File) -> io::Result<PathBuf> {
    let fd = file.as_raw_fd();
    let path = format!("/proc/self/fd/{}", fd);
    let path_str = std::fs::read_link(&path)?;
    Ok(path_str)
}

#[cfg(not(target_os = "linux"))]
fn get_file_path(_file: &File) -> io::Result<PathBuf> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Not supported on this platform",
    ))
}

const OFF_WIDTH: u64 = 4;
const POS_WIDTH: u64 = 8;
const ENT_WIDTH: u64 = OFF_WIDTH + POS_WIDTH;

struct Index {
    file: File,
    mmap: MmapMut,
    size: u64,
}

impl Index {
    fn new(file: File, config: Config) -> Result<Index, std::io::Error> {
        let fi = file.metadata()?;
        let size = fi.size();
        file.set_len(config.segment.max_index_bytes)?;
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        Ok(Index { file, mmap, size })
    }

    fn close(&self) -> Result<(), std::io::Error> {
        self.mmap.flush()?;
        self.file.set_len(self.size)?;
        self.file.sync_all()?;
        Ok(())
    }

    fn read(&self, inp: i64) -> Result<(u32, u64), std::io::ErrorKind> {
        if self.size == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof);
        }
        let mut out: u32 = 0;
        if inp == -1 {
            out = (self.size as u32 / ENT_WIDTH as u32) - 1;
        } else {
            out = inp as u32;
        }
        let mut pos = out as u64 * ENT_WIDTH;
        if self.size < pos + ENT_WIDTH {
            return Err(std::io::ErrorKind::UnexpectedEof);
        }
        out = BigEndian::read_u32(&self.mmap[pos as usize..pos as usize + OFF_WIDTH as usize]);
        pos = BigEndian::read_u64(
            &self.mmap[pos as usize + OFF_WIDTH as usize..pos as usize + ENT_WIDTH as usize],
        );
        Ok((out as u32, pos))
    }

    fn write(&mut self, off: u32, pos: u64) -> Result<(), std::io::ErrorKind> {
        if (self.mmap.len() as u64) < self.size + ENT_WIDTH {
            return Err(std::io::ErrorKind::UnexpectedEof);
        }
        BigEndian::write_u32(
            &mut self.mmap[self.size as usize..self.size as usize + OFF_WIDTH as usize],
            off,
        );
        BigEndian::write_u64(
            &mut self.mmap
                [self.size as usize + OFF_WIDTH as usize..self.size as usize + ENT_WIDTH as usize],
            pos,
        );
        self.size += ENT_WIDTH;
        Ok(())
    }

    fn name(&self) -> String {
        let file = get_file_path(&self.file).unwrap();
        file.to_string_lossy().into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal::log::config::Segment;
    use assert2::check;
    use assert2::let_assert;
    use std::fs::OpenOptions;
    use tempfile::NamedTempFile;

    #[test]
    fn test_index() -> io::Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&path)?;

        let config = Config {
            segment: Segment {
                max_index_bytes: 1024,
                max_store_bytes: 1024,
                inital_offset: 1,
            },
        };

        let mut idx = Index::new(file, config.clone())?;
        check!(idx.read(-1).is_err(), "Reading empty index should fail");

        check!(
            idx.name() == path.to_string_lossy(),
            "Name should match file path"
        );

        let entries = vec![(0u32, 0u64), (1u32, 10u64)];
        for &(want_off, want_pos) in &entries {
            check!(
                idx.write(want_off, want_pos).is_ok(),
                "Write should succeed"
            );
            let (off, pos) = idx.read(want_off as i64)?;
            check!(off == want_off, "Offset should match");
            check!(pos == want_pos, "Position should match");
        }

        let_assert!(
            Err(err) = idx.read(entries.len() as i64),
            "Reading past entries should fail"
        );
        check!(err == io::ErrorKind::UnexpectedEof, "Error should be EOF");

        check!(idx.close().is_ok(), "Close should succeed");

        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let idx = Index::new(file, config)?;
        let (off, pos) = idx.read(-1)?;
        check!(off == 1, "Last offset should be 1");
        check!(pos == entries[1].1, "Last position should match");

        Ok(())
    }
}

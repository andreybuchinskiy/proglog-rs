use crate::internal::log::config::Config;
use crate::internal::log::helpers::get_file_path;
use anyhow::{anyhow, Result};
use byteorder::{BigEndian, ByteOrder};
use memmap2::{MmapMut, MmapOptions};
use std::fs::File;
use std::os::unix::fs::MetadataExt;

pub const OFF_WIDTH: u64 = 4;
pub const POS_WIDTH: u64 = 8;
pub const ENT_WIDTH: u64 = OFF_WIDTH + POS_WIDTH;

pub struct Index {
    pub file: File,
    pub mmap: MmapMut,
    pub size: u64,
}

impl Index {
    pub fn new(file: File, config: Config) -> Result<Index> {
        let fi = file.metadata()?;
        let size = fi.size();
        file.set_len(config.segment.max_index_bytes)?;
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        Ok(Index { file, mmap, size })
    }

    pub fn close(&self) -> Result<()> {
        self.mmap.flush()?;
        self.file.set_len(self.size)?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn read(&self, inp: i64) -> Result<(u32, u64)> {
        if self.size == 0 {
            return Err(anyhow!("Unexpected EOF"));
        }
        let mut out = if inp == -1 {
            (self.size as u32 / ENT_WIDTH as u32) - 1
        } else {
            inp as u32
        };
        let mut pos = (out as u64) * ENT_WIDTH;
        if self.size < pos + ENT_WIDTH {
            return Err(anyhow!("Unexpected EOF"));
        }
        out = BigEndian::read_u32(&self.mmap[pos as usize..pos as usize + OFF_WIDTH as usize]);
        pos = BigEndian::read_u64(
            &self.mmap[pos as usize + OFF_WIDTH as usize..pos as usize + ENT_WIDTH as usize],
        );
        Ok((out, pos))
    }

    pub fn write(&mut self, off: u32, pos: u64) -> Result<()> {
        if (self.mmap.len() as u64) < self.size + ENT_WIDTH {
            return Err(anyhow!("Unexpected EOF"));
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

    pub fn name(&self) -> String {
        let file = get_file_path(&self.file).unwrap();
        file.to_string_lossy().into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal::log::config::SegmentConfig;
    use anyhow::Result;
    use assert2::check;
    use assert2::let_assert;
    use std::fs::OpenOptions;
    use tempfile::NamedTempFile;

    #[test]
    fn test_index() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&path)?;

        let config = Config {
            segment: SegmentConfig {
                max_index_bytes: 1024,
                max_store_bytes: 1024,
                initial_offset: 1,
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
        check!(err.to_string() == "Unexpected EOF", "Error should be EOF");

        check!(idx.close().is_ok(), "Close should succeed");

        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let idx = Index::new(file, config)?;
        let (off, pos) = idx.read(-1)?;
        check!(off == 1, "Last offset should be 1");
        check!(pos == entries[1].1, "Last position should match");

        Ok(())
    }
}

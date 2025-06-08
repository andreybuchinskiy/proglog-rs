pub mod config;
pub mod helpers;
pub mod index;
pub mod segment;
pub mod store;

use anyhow::{anyhow, Result};
use std::fs;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use crate::api::v1::Record;

use config::Config;
use segment::Segment;
use store::Store;

struct Log {
    dir: String,
    config: Config,
    active_segment: Option<Arc<Mutex<Segment>>>,
    segments: Vec<Arc<Mutex<Segment>>>,
}

impl Log {
    pub fn new(dir: String, mut config: Config) -> Result<Log> {
        if config.segment.max_store_bytes == 0 {
            config.segment.max_store_bytes = 1024;
        }
        if config.segment.max_index_bytes == 0 {
            config.segment.max_index_bytes = 1024;
        }
        let mut log = Log {
            dir,
            config,
            active_segment: None,
            segments: Vec::new(),
        };
        log.setup()?;

        Ok(log)
    }

    pub fn setup(&mut self) -> Result<()> {
        let entries = fs::read_dir(&self.dir)?;
        let mut base_offsets: Vec<u64> = entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_name = entry.file_name();
                let name = file_name.to_str()?;
                let off_str = name.trim_end_matches(|c: char| c == '.' || c.is_alphabetic());
                u64::from_str(off_str).ok()
            })
            .collect();
        base_offsets.sort();

        for i in (0..base_offsets.len()).step_by(2) {
            self.new_segment(base_offsets[i])?;
        }

        if self.segments.is_empty() {
            self.new_segment(self.config.segment.initial_offset)?;
        }

        Ok(())
    }

    pub fn append(&mut self, record: Record) -> Result<u64> {
        match &mut self.active_segment {
            Some(active_segment) => {
                let cloned_segment = active_segment.clone();
                let mut writeable_segment = cloned_segment.lock().map_err(|e| {
                    anyhow!("Failed to obtain write lock for active segment: {}", e)
                })?;
                let off = writeable_segment.append(record)?;
                if writeable_segment.is_maxed()? {
                    self.new_segment(off + 1)?;
                }
                Ok(off)
            }
            None => Err(anyhow!("No active segment")),
        }
    }

    pub fn read(&mut self, off: u64) -> Result<Record> {
        let segment = self.segments.iter_mut().find(|s| {
            let segment = s.lock().unwrap();
            segment.base_offset <= off && off < segment.next_offset
        });
        match segment {
            Some(s) => {
                let cloned_segment = s.clone();
                let mut seg = cloned_segment.lock().map_err(|e| anyhow!(e.to_string()))?;
                Ok(seg.read(off)?)
            }
            None => Err(anyhow!("offset out of range: {}", off)),
        }
    }

    pub fn close(&mut self) -> Result<()> {
        self.segments.iter_mut().for_each(|s| {
            s.lock().unwrap().close().unwrap();
        });
        Ok(())
    }

    pub fn remove(&mut self) -> Result<()> {
        self.close()?;
        Ok(fs::remove_dir(&self.dir)?)
    }

    pub fn reset(&mut self) -> Result<()> {
        self.remove()?;
        self.setup()
    }

    pub fn lowest_offset(&mut self) -> Result<u64> {
        Ok(self.segments[0]
            .lock()
            .map_err(|e| anyhow!(e.to_string()))?
            .base_offset)
    }

    pub fn highest_offset(&mut self) -> Result<u64> {
        let off = self.segments[self.segments.len() - 1]
            .lock()
            .map_err(|e| anyhow!(e.to_string()))?
            .next_offset;
        if off == 0 {
            Ok(0)
        } else {
            Ok(off - 1)
        }
    }

    pub fn truncate(&mut self, lowest: u64) -> Result<()> {
        self.segments.retain_mut(|s| {
            let mut segment = s.lock().unwrap();
            if segment.next_offset <= lowest + 1 {
                if segment.remove().is_err() {
                    return false;
                }
                false
            } else {
                true
            }
        });
        Ok(())
    }

    pub fn new_segment(&mut self, off: u64) -> Result<()> {
        let segment = Arc::new(Mutex::new(Segment::new(
            &self.dir,
            off,
            self.config.clone(),
        )?));
        self.segments.push(segment.clone());
        self.active_segment = Some(segment);
        Ok(())
    }

    pub fn reader(&mut self) -> Result<MultiReader> {
        let readers: Vec<OriginReader> = self
            .segments
            .iter_mut()
            .map(|s| OriginReader::new(s.lock().unwrap().store.clone(), 0))
            .collect::<Vec<OriginReader>>();
        Ok(MultiReader::new(readers))
    }
}

struct OriginReader {
    store: Arc<Mutex<Store>>,
    off: u64,
}

impl OriginReader {
    fn new(store: Arc<Mutex<Store>>, off: u64) -> OriginReader {
        OriginReader { store, off }
    }
    fn read(&mut self, p: &mut [u8]) -> Result<usize> {
        let mut store = self.store.lock().map_err(|e| anyhow!(e.to_string()))?;
        let n = &store.read_at(p, self.off)?;
        self.off += *n as u64;
        Ok(*n)
    }
}

struct MultiReader {
    readers: Vec<OriginReader>,
}

impl MultiReader {
    fn new(readers: Vec<OriginReader>) -> MultiReader {
        MultiReader { readers }
    }

    fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut b = [0u8; 1024];
        let mut bytes_read: usize = 0;
        self.readers.iter_mut().for_each(|r| {
            let x = r.read(&mut b).unwrap();
            bytes_read += x;
        });
        Ok(b[0..bytes_read].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::config::{Config, SegmentConfig};
    use super::{Log, Record};
    use crate::internal::log::store::LEN_WIDTH;
    use anyhow::Result;
    use assert2::let_assert;
    use tempfile::{tempdir, TempDir};

    fn create_log() -> Result<(Log, TempDir)> {
        let temp_dir = tempdir()?;
        let dir = temp_dir.path().to_path_buf();
        let config = Config {
            segment: SegmentConfig {
                max_store_bytes: 32,
                initial_offset: 0,
                max_index_bytes: 0,
            },
        };
        let log = Log::new((dir.to_string_lossy()).to_string(), config)?;
        Ok((log, temp_dir))
    }

    fn create_record() -> Record {
        Record {
            value: "hello world".into(),
            offset: 0,
        }
    }

    #[test]
    fn test_append_read() -> Result<()> {
        let (mut log, _tmpdir) = create_log()?;
        let record = create_record();
        let off = log.append(record.clone())?;
        assert_eq!(off, 0);
        let read = log.read(off)?;
        assert_eq!(read.value, record.value);
        Ok(())
    }

    #[test]
    fn test_out_of_range() -> Result<()> {
        let (mut log, _) = create_log()?;
        let_assert!(Err(_) = log.read(1));
        Ok(())
    }

    #[test]
    fn test_init_existing() -> Result<()> {
        let (mut log, _tmpdir) = create_log()?;
        let record = create_record();
        let mut i = 0;
        while i < 3 {
            log.append(record.clone())?;
            i += 1;
        }
        let off = log.lowest_offset()?;
        assert_eq!(off, 0);
        let off = log.highest_offset()?;
        assert_eq!(off, 2);
        log = Log::new(log.dir, log.config)?;
        let off = log.lowest_offset()?;
        assert_eq!(off, 0);
        let off = log.highest_offset()?;
        assert_eq!(off, 2);
        Ok(())
    }

    #[test]
    fn test_reader() -> Result<()> {
        let (mut log, _tmpdir) = create_log()?;
        let record = create_record();
        let off = log.append(record.clone())?;
        assert_eq!(off, 0);
        let mut reader = log.reader()?;
        let b = reader.read_all()?;
        let read: Record = serde_json::from_slice(&b[LEN_WIDTH..])?;
        assert_eq!(read.value, record.value);
        Ok(())
    }

    #[test]
    fn test_truncate() -> Result<()> {
        let (mut log, _tmpdir) = create_log()?;
        let record = create_record();
        let mut i = 0;
        while i < 3 {
            log.append(record.clone())?;
            i += 1;
        }
        log.truncate(0)?;
        let_assert!(Err(_) = log.read(0));
        Ok(())
    }
}

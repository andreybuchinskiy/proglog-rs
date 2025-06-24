pub mod config;
pub mod helpers;
pub mod index;
pub mod segment;
pub mod store;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex as SyncMutex;
use tokio::sync::Mutex;

use crate::api::v1::Record;
use crate::internal::server::CommitLog;

use config::Config;
use segment::Segment;
use store::Store;

pub struct Log {
    dir: String,
    config: Config,
    active_segment: Option<Arc<Mutex<Segment>>>,
    segments: Vec<Arc<Mutex<Segment>>>,
}

#[async_trait]
impl CommitLog for Log {
    async fn append(&mut self, record: Record) -> Result<u64> {
        match &mut self.active_segment {
            Some(active_segment) => {
                let cloned_segment = active_segment.clone();
                let mut writeable_segment = cloned_segment.lock().await;
                let off = writeable_segment.append(record)?;
                if writeable_segment.is_maxed()? {
                    self.new_segment(off + 1).await?;
                }
                Ok(off)
            }
            None => Err(anyhow!("No active segment")),
        }
    }
    async fn read(&mut self, off: u64) -> Result<Record> {
        let mut segment = None;
        for s in self.segments.iter_mut() {
            let seg = s.lock().await;
            if seg.base_offset <= off && off < seg.next_offset {
                segment = Some(s.clone())
            }
        }
        match segment {
            Some(s) => {
                let cloned_segment = s.clone();
                let mut seg = cloned_segment.lock().await;
                Ok(seg.read(off)?)
            }
            None => Err(anyhow!("offset out of range: {}", off)),
        }
    }
}

impl Log {
    pub async fn new(dir: String, mut config: Config) -> Result<Log> {
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
        log.setup().await?;

        Ok(log)
    }

    pub async fn setup(&mut self) -> Result<()> {
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
            self.new_segment(base_offsets[i]).await?;
        }

        if self.segments.is_empty() {
            self.new_segment(self.config.segment.initial_offset).await?;
        }

        Ok(())
    }

    // pub async fn append(&mut self, record: Record) -> Result<u64> {
    //     match &mut self.active_segment {
    //         Some(active_segment) => {
    //             let cloned_segment = active_segment.clone();
    //             let mut writeable_segment = cloned_segment.lock().map_err(|e| {
    //                 anyhow!("Failed to obtain write lock for active segment: {}", e)
    //             })?;
    //             let off = writeable_segment.append(record)?;
    //             if writeable_segment.is_maxed()? {
    //                 self.new_segment(off + 1)?;
    //             }
    //             Ok(off)
    //         }
    //         None => Err(anyhow!("No active segment")),
    //     }
    // }

    // pub async fn read(&mut self, off: u64) -> Result<Record> {
    //     let segment = self.segments.iter_mut().find(|s| {
    //         let segment = s.lock().unwrap();
    //         segment.base_offset <= off && off < segment.next_offset
    //     });
    //     match segment {
    //         Some(s) => {
    //             let cloned_segment = s.clone();
    //             let mut seg = cloned_segment.lock().map_err(|e| anyhow!(e.to_string()))?;
    //             Ok(seg.read(off)?)
    //         }
    //         None => Err(anyhow!("offset out of range: {}", off)),
    //     }
    // }

    pub async fn close(&mut self) -> Result<()> {
        for s in self.segments.iter_mut() {
            s.lock().await.close().map_err(|e| anyhow!(e.to_string()))?
        }
        Ok(())
    }

    pub async fn remove(&mut self) -> Result<()> {
        self.close().await?;
        Ok(fs::remove_dir(&self.dir)?)
    }

    pub async fn reset(&mut self) -> Result<()> {
        self.remove().await?;
        self.setup().await
    }

    pub async fn lowest_offset(&mut self) -> Result<u64> {
        Ok(self.segments[0].lock().await.base_offset)
    }

    pub async fn highest_offset(&mut self) -> Result<u64> {
        let off = self.segments[self.segments.len() - 1]
            .lock()
            .await
            .next_offset;
        if off == 0 {
            Ok(0)
        } else {
            Ok(off - 1)
        }
    }

    pub async fn truncate(&mut self, lowest: u64) -> Result<()> {
        let mut segments = Vec::new();
        for s in self.segments.iter_mut() {
            let mut seg = s.lock().await;
            if seg.next_offset <= lowest + 1 {
                seg.remove()?
            } else {
                segments.push(s.clone());
            }
        }
        self.segments = segments;
        Ok(())
    }

    pub async fn new_segment(&mut self, off: u64) -> Result<()> {
        let segment = Arc::new(Mutex::new(Segment::new(
            &self.dir,
            off,
            self.config.clone(),
        )?));
        self.segments.push(segment.clone());
        self.active_segment = Some(segment);
        Ok(())
    }

    pub async fn reader(&mut self) -> Result<MultiReader> {
        let mut readers = Vec::new();
        for s in self.segments.iter_mut() {
            let seg = s.lock().await;
            readers.push(OriginReader::new(seg.store.clone(), 0));
        }
        Ok(MultiReader::new(readers))
    }
}

pub struct OriginReader {
    store: Arc<SyncMutex<Store>>,
    off: u64,
}

impl OriginReader {
    pub fn new(store: Arc<SyncMutex<Store>>, off: u64) -> OriginReader {
        OriginReader { store, off }
    }
    pub fn read(&mut self, p: &mut [u8]) -> Result<usize> {
        let mut store = self.store.lock().map_err(|e| anyhow!(e.to_string()))?;
        let n = &store.read_at(p, self.off)?;
        self.off += *n as u64;
        Ok(*n)
    }
}

pub struct MultiReader {
    readers: Vec<OriginReader>,
}

impl MultiReader {
    pub fn new(readers: Vec<OriginReader>) -> MultiReader {
        MultiReader { readers }
    }

    pub fn read_all(&mut self) -> Result<Vec<u8>> {
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
    use crate::internal::server::CommitLog;
    use anyhow::Result;
    use assert2::let_assert;
    use tempfile::{tempdir, TempDir};

    async fn create_log() -> Result<(Log, TempDir)> {
        let temp_dir = tempdir()?;
        let dir = temp_dir.path().to_path_buf();
        let config = Config {
            segment: SegmentConfig {
                max_store_bytes: 32,
                initial_offset: 0,
                max_index_bytes: 0,
            },
        };
        let log = Log::new((dir.to_string_lossy()).to_string(), config).await?;
        Ok((log, temp_dir))
    }

    fn create_record() -> Record {
        Record {
            value: "hello world".into(),
            offset: 0,
        }
    }

    #[tokio::test]
    async fn test_append_read() -> Result<()> {
        let (mut log, _tmpdir) = create_log().await?;
        let record = create_record();
        let off = log.append(record.clone()).await?;
        assert_eq!(off, 0);
        let read = log.read(off).await?;
        assert_eq!(read.value, record.value);
        Ok(())
    }

    #[tokio::test]
    async fn test_out_of_range() -> Result<()> {
        let (mut log, _) = create_log().await?;
        let_assert!(Err(_) = log.read(1).await);
        Ok(())
    }

    #[tokio::test]
    async fn test_init_existing() -> Result<()> {
        let (mut log, _tmpdir) = create_log().await?;
        let record = create_record();
        let mut i = 0;
        while i < 3 {
            log.append(record.clone()).await?;
            i += 1;
        }
        let off = log.lowest_offset().await?;
        assert_eq!(off, 0);
        let off = log.highest_offset().await?;
        assert_eq!(off, 2);
        log = Log::new(log.dir, log.config).await?;
        let off = log.lowest_offset().await?;
        assert_eq!(off, 0);
        let off = log.highest_offset().await?;
        assert_eq!(off, 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_reader() -> Result<()> {
        let (mut log, _tmpdir) = create_log().await?;
        let record = create_record();
        let off = log.append(record.clone()).await?;
        assert_eq!(off, 0);
        let mut reader = log.reader().await?;
        let b = reader.read_all()?;
        let read: Record = serde_json::from_slice(&b[LEN_WIDTH..])?;
        assert_eq!(read.value, record.value);
        Ok(())
    }

    #[tokio::test]
    async fn test_truncate() -> Result<()> {
        let (mut log, _tmpdir) = create_log().await?;
        let record = create_record();
        let mut i = 0;
        while i < 3 {
            log.append(record.clone()).await?;
            i += 1;
        }
        log.truncate(0).await?;
        let_assert!(Err(_) = log.read(0).await);
        Ok(())
    }
}

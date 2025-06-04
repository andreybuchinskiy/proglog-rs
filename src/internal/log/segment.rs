use crate::api::v1::Record;
use crate::internal::log::config::Config;
use crate::internal::log::index::Index;
use crate::internal::log::store::Store;
use std::fs::{remove_file, OpenOptions};
use std::io;

pub struct Segment {
    store: Store,
    index: Index,
    base_offset: u64,
    next_offset: u64,
    config: Config,
}

impl Segment {
    fn new(dir: &str, base_offset: u64, config: Config) -> Result<Segment, io::Error> {
        let store_path = format!("{}/{}.store", dir, base_offset);
        let store_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(store_path)?;
        let store = Store::new_store(store_file)?;
        let index_path = format!("{}/{}.index", dir, base_offset);
        let index_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(index_path)?;
        let index = Index::new(index_file, config.clone())?;
        let mut next_offset: u64 = 0;
        match index.read(-1) {
            Ok((off, _)) => next_offset = base_offset + off as u64 + 1,
            Err(_) => next_offset = base_offset,
        };
        Ok(Segment {
            store,
            index,
            base_offset,
            next_offset,
            config,
        })
    }

    fn append(&mut self, mut record: Record) -> Result<u64, io::Error> {
        let cur = self.next_offset;
        record.offset = cur;
        let p = serde_json::to_vec(&record)?;
        let (_, pos) = self.store.append(&p)?;
        let off: u32 = self.next_offset as u32 - self.base_offset as u32;
        self.index.write(off, pos)?;
        self.next_offset += 1;
        Ok(cur)
    }

    fn read(&mut self, off: u64) -> Result<Record, io::Error> {
        let offset = off - self.base_offset;
        let (_, pos) = self.index.read(offset as i64)?;
        let p = self.store.read(pos)?;
        let record: Record = serde_json::from_slice(&p)?;
        Ok(record)
    }

    fn is_maxed(&self) -> bool {
        self.store.size >= self.config.segment.max_store_bytes
            || self.index.size >= self.config.segment.max_index_bytes
    }

    fn remove(&mut self) -> Result<(), io::Error> {
        self.close()?;
        remove_file(self.index.name())?;
        remove_file(self.store.name())?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), io::Error> {
        self.index.close()?;
        self.store.close()?;
        Ok(())
    }

    fn nearest_multiple(&self, j: u64, k: u64) -> u64 {
        (j / k) * k
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, Record, Segment};
    use crate::internal::log::config::SegmentConfig;
    use crate::internal::log::index::ENT_WIDTH;
    use assert2::check;
    use assert2::let_assert;
    use std::io;
    use tempfile::tempdir;

    #[test]
    fn test_segment() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let file_path = temp_dir.path().to_path_buf();
        let want = Record {
            value: "hello world".into(),
            offset: 0,
        };
        let mut config = Config {
            segment: SegmentConfig {
                max_store_bytes: 1024,
                max_index_bytes: 3 * ENT_WIDTH,
                initial_offset: 0,
            },
        };
        let mut segment = Segment::new(&file_path.to_string_lossy(), 16, config.clone())?;
        check!(16 == segment.next_offset, "Next offset should be 16");
        check!(segment.is_maxed() == false, "is_maxed should be false");
        let mut i = 0;
        while i < 3 {
            let off = segment.append(want.clone())?;
            check!(16 + i == off, "Offset does not match");
            let got = segment.read(off)?;
            check!(
                want.value == got.value,
                "Did not get the same value we wrote"
            );
            i += 1;
        }

        let_assert!(
            Err(_) = segment.append(want.clone()),
            "Writing a third time should fail"
        );

        check!(segment.is_maxed() == true, "Segment should be maxed");

        config.segment.max_store_bytes = want.value.len() as u64 * 3;
        config.segment.max_index_bytes = 1024;

        segment = Segment::new(&file_path.to_string_lossy(), 16, config.clone())?;
        check!(segment.is_maxed() == true, "segment should be maxed");

        segment.remove()?;
        segment = Segment::new(&file_path.to_string_lossy(), 16, config.clone())?;
        check!(segment.is_maxed() == false, "segment should not be maxed");

        Ok(())
    }
}

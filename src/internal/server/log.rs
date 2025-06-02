use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::Mutex;

fn from_base64<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let s: String = Deserialize::deserialize(deserializer)?;
    general_purpose::STANDARD
        .decode(&s)
        .map_err(D::Error::custom)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    #[serde(deserialize_with = "from_base64")]
    value: Vec<u8>,
    offset: u64,
}

pub struct Log {
    records: Mutex<Vec<Record>>,
}

#[derive(Debug)]
struct OffsetNotFound;

impl std::fmt::Display for OffsetNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "offset not found")
    }
}

impl Default for Log {
    fn default() -> Log {
        Log {
            records: Mutex::new(Vec::new()),
        }
    }
}

impl std::error::Error for OffsetNotFound {}

impl Log {
    pub fn append(&self, record: Record) -> Result<u64, Box<dyn std::error::Error + '_>> {
        let mut records = self
            .records
            .lock()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        let offset = records.len() as u64;
        let mut record = record;
        record.offset = offset;
        records.push(record);
        Ok(offset)
    }

    pub fn read(&self, offset: u64) -> Result<Record, Box<dyn std::error::Error + '_>> {
        let records = self
            .records
            .lock()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        if offset >= records.len() as u64 {
            return Err(Box::new(OffsetNotFound));
        }
        Ok(records[offset as usize].clone())
    }
}

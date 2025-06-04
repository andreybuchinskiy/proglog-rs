#[derive(Clone)]
pub struct Config {
    pub segment: SegmentConfig,
}

#[derive(Clone)]
pub struct SegmentConfig {
    pub max_store_bytes: u64,
    pub max_index_bytes: u64,
    pub initial_offset: u64,
}

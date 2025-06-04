#[derive(Clone)]
pub struct Config {
    pub segment: Segment,
}

#[derive(Clone)]
pub struct Segment {
    pub max_store_bytes: u64,
    pub max_index_bytes: u64,
    pub inital_offset: u64,
}

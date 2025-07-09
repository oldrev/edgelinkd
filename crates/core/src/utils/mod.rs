use std::time::{SystemTime, UNIX_EPOCH};

pub mod async_util;

#[allow(clippy::all)]
pub mod graph;

pub mod time;
pub mod topo;

pub fn generate_uid() -> u64 {
    let mut rng = rand::rng();
    let random_part: u64 = rand::RngCore::next_u64(&mut rng);
    let timestamp_part = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time error!!!").as_nanos() as u64;

    timestamp_part ^ random_part
}

pub fn generate_str_uid() -> String {
    format!("{:016x}", generate_uid())
}

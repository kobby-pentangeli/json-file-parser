use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::fs::File;
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct OraoStream {
    pub provider_id: u32,
    pub key: u64,
    pub value: f64,
    pub timestamp: u128,
}

fn main() {
    let json_file_path = Path::new("../data.json");
    let file = File::open(json_file_path).expect("file not found");
    let packets: Vec<OraoStream> = serde_json::from_reader(file).expect("error while reading file");

    // remove duplicates
    let uniques = packets
        .clone()
        .into_iter()
        .dedup_by(|x, y| x.timestamp - y.timestamp <= 100)
        .collect::<Vec<OraoStream>>();

    let good_lines = uniques.len();
    let maximum_gap = packets.len() - uniques.len();
    let average_gap = packets.len() / maximum_gap;

    println!("-----------------------------------------------------------------------------------");
    println!(
        "| Time elapsed: {:.2} | Amount of good lines: {} |  Maximum gap: {} |  Average gap: {} |",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs_f64(),
        good_lines,
        maximum_gap,
        average_gap
    );
    println!("-----------------------------------------------------------------------------------");
}

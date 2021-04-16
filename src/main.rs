//! JSON file parser to aid in calculating data-quality metrics
//! Loads JSON files and calculates 3 metrics: amount of good lines, maximum gap, and average gap.
//! Also calculates and prints the metrics and time which was taken to calculate them.

use itertools::Itertools;
use serde::Deserialize;
use std::fs;
use std::time::{Duration, SystemTime};

/// Structure of a block of JSON data
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Message {
    pub provider_id: i32,
    pub key: i32,
    pub value: f32,
    pub timestamp: i64,
}

fn main() -> std::io::Result<()> {
    for entry in fs::read_dir("./stream")? {
        let dir = entry?;
        let path = dir.path();
        let file = fs::File::open(path).expect("File not found");
        let data: Vec<Message> = serde_json::from_reader(file).expect("Error while reading");

        // Remove duplicates
        let uniques = data
            .clone()
            .into_iter()
            .dedup_by(|x, y| x.provider_id == y.provider_id && x.key == y.key)
            .collect::<Vec<Message>>();

        // Calculate metrics
        // TODO:__ 
        let good_lines = uniques.len();
        let maximum_gap = data.len() - uniques.len();
        let average_gap = data.len() / maximum_gap;

        println!(
            "-----------------------------------------------------------------------------------------"
        );
        println!(
            "| Time elapsed: {:.2} | Amount of good lines: {} |  Maximum gap: {} |  Average gap: {} |",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_else(|_| Duration::from_secs(0))
                .as_secs_f64(),
            good_lines,
            maximum_gap,
            average_gap
        );
        println!(
            "-----------------------------------------------------------------------------------------"
        );

        println!("{:?}", dir.path());
    }
    Ok(())
}

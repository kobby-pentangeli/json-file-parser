//! JSON file parser to aid in calculating data-quality metrics
//! Loads JSON files and calculates 3 metrics: amount of good lines, maximum gap, and average gap.
//! Also calculates and prints the metrics and time which was taken to calculate them.

use itertools::Itertools;
use serde::Deserialize;
use std::fs;
use std::time::{Duration, SystemTime};

fn main() -> std::io::Result<()> {
    for entry in fs::read_dir("./stream")? {
        let dir = entry?;
        let path = dir.path();
        let file = fs::File::open(path).expect("File not found");
        let data: Vec<Message> = serde_json::from_reader(file).expect("Error while reading");

        // Calculate metrics
        let maximum_gap = calculate_max_gap(&data).unwrap();
        let average_gap = calculate_avg_gap(&data);
        let uniques = remove_duplicates(data);
        let good_lines = uniques.len();

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

/// Structure of a block of JSON data
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Message {
    provider_id: i32,
    key: i32,
    value: f32,
    timestamp: i64,
}

/// Removes duplicate lines with respect to same provider ID and key
pub fn remove_duplicates(data: Vec<Message>) -> Vec<Message> {
    data
        .into_iter()
        .dedup_by(|x, y| x.provider_id == y.provider_id && x.key == y.key)
        .collect::<Vec<Message>>()
}

/// Computes the largest gap between two timestamps
pub fn calculate_max_gap(data: &[Message]) -> Option<i64> {
    data.windows(2)
            .map(|message| (message[0].timestamp - message[1].timestamp).abs())
            .max_by(|x, y| x.partial_cmp(y).unwrap())
}

/// Computes the average on all gaps
pub fn calculate_avg_gap(data: &[Message]) -> i64 {
    data.windows(2)
        .map(|message| (message[0].timestamp - message[1].timestamp).abs())
        .sum::<i64>()
}
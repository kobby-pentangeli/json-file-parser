//! JSON file parser to aid in calculating data-quality metrics
//! Loads JSON files and calculates 3 metrics: amount of good lines, maximum gap, and average gap.
//! Also calculates and prints the metrics and time which was taken to calculate them.

use itertools::Itertools;
use serde::Deserialize;
use std::fs::File;
use std::time::{Duration, SystemTime};
use walkdir::WalkDir;

/// Structure of a block of JSON data
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Message {
    pub provider_id: i32,
    pub key: i32,
    pub value: f32,
    pub timestamp: i64,
}

fn main() -> std::io::Result<()> {
    // Extract files
    let mut files: Vec<File> = Vec::new();
    for entry in WalkDir::new("../stream").into_iter().filter_map(|e| e.ok()) {
        let json_file_path = entry.path();
        let file = File::open(json_file_path).expect("File not found");
        files.push(file);
    }

    // Map files to `Message` structs
    let mut packets: Vec<Vec<Message>> = Vec::new();
    for file in files {
        let data: Vec<Message> = serde_json::from_reader(file).expect("Error while reading");
        packets.push(data);
    }

    // Iterate over the list of files
    for data in &packets {
        // Remove duplicates
        let uniques = data
            .clone()
            .into_iter()
            .dedup_by(|x, y| x.timestamp - y.timestamp <= 100)
            .collect::<Vec<Message>>();

        // Calculate metrics
        let good_lines = uniques.len();
        let maximum_gap = &packets.len() - uniques.len();
        let average_gap = packets.len() / maximum_gap;

        println!(
            "-----------------------------------------------------------------------------------------"
        );
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
        println!(
            "-----------------------------------------------------------------------------------------"
        );
    }
    Ok(())
}

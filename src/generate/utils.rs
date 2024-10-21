use crate::generate::mappings::SCD;
use chrono::NaiveDate;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub fn load_json(json_file: &Path) -> Result<Value, Box<dyn std::error::Error>> {
    let json_str = fs::read_to_string(json_file)?;
    let json: Value = serde_json::from_str(&json_str)?;
    Ok(json)
}

pub fn load_mapping<K: std::str::FromStr + std::hash::Hash + Eq, V: DeserializeOwned + Clone>(
    file_path: &str,
) -> HashMap<K, V> {
    let file_content =
        fs::read_to_string(file_path).expect(&format!("Failed to read file: {}", file_path));
    let json_map: HashMap<String, V> = serde_json::from_str(&file_content)
        .expect(&format!("Failed to parse JSON from file: {}", file_path));

    json_map
        .into_iter()
        .filter_map(|(k, v)| K::from_str(&k).ok().map(|parsed_k| (parsed_k, v)))
        .collect()
}

pub fn generate_date_for_year(year: i32) -> String {
    let mut rng = rand::thread_rng();
    let month = rng.gen_range(1..13);
    let day = rng.gen_range(1..29);
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .format("%Y-%m-%d")
        .to_string()
}

pub fn get_random_diagnosis() -> String {
    let mut rng = rand::thread_rng();

    if rng.gen_bool(0.1) {
        // 10% chance of using SCD mapping
        return format!(
            "D{}",
            SCD.keys()
                .collect::<Vec<_>>()
                .choose(&mut rng)
                .unwrap()
                .to_string()
        );
    }

    // 90% chance of generating a new code
    let chapter_letters = [
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R",
        "S", "T", "U", "V", "W", "X", "Y", "Z",
    ];

    let letter = chapter_letters.choose(&mut rng).unwrap();
    let first_number: u8 = rng.gen_range(0..=99);
    let second_number: u8 = rng.gen_range(0..=9);

    let third_part = if rng.gen_bool(0.3) {
        // 30% chance of adding a letter
        let additional_letter = (b'A' + rng.gen_range(0..26)) as char;
        additional_letter.to_string()
    } else {
        // 70% chance of adding a number
        rng.gen_range(0..=9).to_string()
    };

    if second_number == 0 && third_part == "0" {
        format!("D{}{:02}", letter, first_number)
    } else if third_part == "0" {
        format!("D{}{:02}{}", letter, first_number, second_number)
    } else {
        format!(
            "D{}{:02}{}{}",
            letter, first_number, second_number, third_part
        )
    }
}

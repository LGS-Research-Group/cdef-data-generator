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

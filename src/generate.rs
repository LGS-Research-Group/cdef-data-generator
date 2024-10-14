use once_cell::sync::Lazy;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;

use polars::prelude::*;
use rayon::prelude::*;

pub fn load_json(json_file: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let json_str = fs::read_to_string(json_file)?;
    let json: Value = serde_json::from_str(&json_str)?;
    Ok(json)
}

fn load_mapping<K: std::str::FromStr + std::hash::Hash + Eq, V: DeserializeOwned + Clone>(
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

static SOCIO13: Lazy<HashMap<i32, String>> = Lazy::new(|| load_mapping("mappings/socio13.json"));
static CIVST: Lazy<HashMap<String, String>> = Lazy::new(|| load_mapping("mappings/civst.json"));
static FM_MARK: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/fm_mark.json"));
static HUSTYPE: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/hustype.json"));
static PLADS: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/plads.json"));
static REG: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/reg.json"));
static STATSB: Lazy<HashMap<i32, String>> = Lazy::new(|| load_mapping("mappings/statsb.json"));

pub fn generate_from_json(
    json_file: &str,
    no_rows: usize,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let json = load_json(json_file)?;

    let mut columns = Vec::new();

    if let Some(columns_def) = json.get("columns").and_then(|c| c.as_array()) {
        let is_akm_schema = columns_def.iter().any(|col| {
            col.get("name")
                .and_then(|n| n.as_str())
                .map_or(false, |name| name == "SOCIO13")
        });

        for col_def in columns_def {
            let col_name = col_def
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_default();

            let series = if is_akm_schema {
                create_akm_series(col_name, no_rows)
            } else {
                create_bef_series(col_name, no_rows)
            };
            columns.push(series);
        }
    }
    Ok(DataFrame::new(columns)?)
}

fn create_akm_series(col_name: &str, no_rows: usize) -> Series {
    let col_name = PlSmallStr::from(col_name);

    match col_name.as_str() {
        "PNR" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    format!(
                        "{:010}",
                        rand::thread_rng().gen_range(100000000_u32..999999999_u32)
                    )
                })
                .collect();
            Series::new(col_name, data)
        }
        "SOCIO" | "SOCIO02" | "SOCIO13" => {
            let socio_keys: Vec<i32> = SOCIO13.keys().cloned().collect();
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| *socio_keys.choose(&mut rand::thread_rng()).unwrap())
                .collect();
            Series::new(col_name, data)
        }
        "CPRTJEK" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    ["V", "U"]
                        .choose(&mut rand::thread_rng())
                        .unwrap()
                        .to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "CPRTYPE" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    ["A", "B", "C", "D", "E", "F"]
                        .choose(&mut rand::thread_rng())
                        .unwrap()
                        .to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "VERSION" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| format!("{:04}", rand::thread_rng().gen_range(2000..2023)))
                .collect();
            Series::new(col_name, data)
        }
        "SENR" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| format!("{:06}", rand::thread_rng().gen_range(100000..999999)))
                .collect();
            Series::new(col_name, data)
        }
        _ => panic!("Unsupported AKM column: {}", col_name),
    }
}

fn create_bef_series(col_name: &str, no_rows: usize) -> Series {
    let col_name = PlSmallStr::from(col_name);

    match col_name.as_str() {
        "AEGTE_ID" | "E_FAELLE_ID" | "FAMILIE_ID" | "FAR_ID" | "MOR_ID" | "PNR" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    format!(
                        "{:010}",
                        rand::thread_rng().gen_range(100000000_u32..999999999_u32)
                    )
                })
                .collect();
            Series::new(col_name, data)
        }
        "ALDER" | "ANTBOERNF" | "ANTBOERNH" | "ANTPERSF" | "ANTPERSH" => {
            let data: Vec<i8> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(0..100))
                .collect();
            Series::new(col_name, data)
        }
        "BOP_VFRA" | "FOED_DAG" => {
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let year = rand::thread_rng().gen_range(1900..2023);
                    let month = rand::thread_rng().gen_range(1..13);
                    let day = rand::thread_rng().gen_range(1..29);
                    year * 10000 + month * 100 + day
                })
                .collect();
            Series::new(col_name, data)
        }
        "CIVST" => {
            let keys: Vec<String> = CIVST.keys().cloned().collect();
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| keys.choose(&mut rand::thread_rng()).unwrap().clone())
                .collect();
            Series::new(col_name, data)
        }
        "CPRTJEK" | "CPRTYPE" => {
            let data: Vec<i8> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(0..2))
                .collect();
            Series::new(col_name, data)
        }
        "FAMILIE_TYPE" => {
            let data: Vec<i16> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(1..10))
                .collect();
            Series::new(col_name, data)
        }
        "FM_MARK" => {
            let keys: Vec<i8> = FM_MARK.keys().cloned().collect();
            let data: Vec<i8> = (0..no_rows)
                .into_par_iter()
                .map(|_| *keys.choose(&mut rand::thread_rng()).unwrap())
                .collect();
            Series::new(col_name, data)
        }
        "HUSTYPE" => {
            let keys: Vec<i8> = HUSTYPE.keys().cloned().collect();
            let data: Vec<i8> = (0..no_rows)
                .into_par_iter()
                .map(|_| *keys.choose(&mut rand::thread_rng()).unwrap())
                .collect();
            Series::new(col_name, data)
        }
        "IE_TYPE" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    ["I", "E"]
                        .choose(&mut rand::thread_rng())
                        .unwrap()
                        .to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "KOEN" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    ["M", "K"]
                        .choose(&mut rand::thread_rng())
                        .unwrap()
                        .to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "KOM" => {
            let data: Vec<i16> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(101..851))
                .collect();
            Series::new(col_name, data)
        }
        "OPR_LAND" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| format!("{:03}", rand::thread_rng().gen_range(1..999)))
                .collect();
            Series::new(col_name, data)
        }
        "PLADS" => {
            let keys: Vec<i8> = PLADS.keys().cloned().collect();
            let data: Vec<i8> = (0..no_rows)
                .into_par_iter()
                .map(|_| *keys.choose(&mut rand::thread_rng()).unwrap())
                .collect();
            Series::new(col_name, data)
        }
        "REG" => {
            let keys: Vec<i8> = REG.keys().cloned().collect();
            let data: Vec<i8> = (0..no_rows)
                .into_par_iter()
                .map(|_| *keys.choose(&mut rand::thread_rng()).unwrap())
                .collect();
            Series::new(col_name, data)
        }
        "STATSB" => {
            let keys: Vec<i32> = STATSB.keys().cloned().collect();
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| *keys.choose(&mut rand::thread_rng()).unwrap())
                .collect();
            Series::new(col_name, data)
        }
        "VERSION" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| format!("{:04}", rand::thread_rng().gen_range(2000..2023)))
                .collect();
            Series::new(col_name, data)
        }
        _ => panic!("Unsupported column: {}", col_name),
    }
}

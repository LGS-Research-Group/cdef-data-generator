use fake::{Fake, Faker};
use once_cell::sync::Lazy;
use rand::seq::SliceRandom;
use rand::Rng;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;

use polars::prelude::*;
use rayon::prelude::*;

use fake::faker::address::raw::*;
use fake::faker::company::raw::*;
use fake::faker::internet::raw::*;
use fake::faker::name::raw::*;
use fake::faker::phone_number::raw::*;
use fake::locales::*;

pub fn load_json(json_file: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let json_str = fs::read_to_string(json_file)?;
    let json: Value = serde_json::from_str(&json_str)?;
    Ok(json)
}

static SOCIO13: Lazy<HashMap<i32, String>> = Lazy::new(|| {
    let file_content = fs::read_to_string("mappings/socio13_mapping.json")
        .expect("Failed to read SOCIO13 mapping file");
    let json_map: HashMap<String, String> =
        serde_json::from_str(&file_content).expect("Failed to parse SOCIO13 mapping JSON");

    json_map
        .into_iter()
        .map(|(k, v)| (k.parse::<i32>().unwrap_or_default(), v))
        .collect()
});

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
            let col_type = col_def
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or_default();

            let series = if is_akm_schema {
                create_akm_series(col_name, no_rows)
            } else {
                create_series_from_type(col_type, col_name, no_rows, EN)
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

fn create_series_from_type<L>(type_name: &str, col_name: &str, no_rows: usize, locale: L) -> Series
where
    L: Data + Sync + Send + Copy,
{
    let col_name = PlSmallStr::from(col_name);

    match type_name {
        "u64" => {
            let data = (0..no_rows)
                .into_par_iter()
                .map(|_| Faker.fake::<u64>())
                .collect::<Vec<u64>>();
            Series::new(col_name, data)
        }
        "FirstName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| FirstName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "LastName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| LastName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "FreeEmail" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| FreeEmail(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "CompanyName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| CompanyName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "PhoneNumber" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| PhoneNumber(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        "StreetName" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| StreetName(locale).fake::<String>())
                .collect();
            Series::new(col_name, data)
        }
        _ => panic!("Unsupported type: {}", type_name),
    }
}

use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};
use once_cell::sync::Lazy;
//use rand::distributions::{Distribution, Uniform};
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

// AKM
static SOCIO13: Lazy<HashMap<i32, String>> = Lazy::new(|| load_mapping("mappings/socio13.json"));
// BEF
static CIVST: Lazy<HashMap<String, String>> = Lazy::new(|| load_mapping("mappings/civst.json"));
static FM_MARK: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/fm_mark.json"));
static HUSTYPE: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/hustype.json"));
static PLADS: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/plads.json"));
static REG: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/reg.json"));
static STATSB: Lazy<HashMap<i32, String>> = Lazy::new(|| load_mapping("mappings/statsb.json"));
// IDAN
static JOBKAT: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/jobkat.json"));
static TILKNYT: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/tilknyt.json"));
static STILL: Lazy<Vec<String>> = Lazy::new(|| {
    vec![
        "01", "02", "03", "04", "05", "11", "12", "13", "14", "19", "20", "31", "32", "33", "34",
        "35", "36", "37", "40", "41", "42", "43", "45", "46", "47", "48", "49", "50", "51", "52",
        "55", "71", "72", "73", "74", "75", "76", "77", "90", "91", "92", "93", "94", "95", "96",
        "97", "98",
    ]
    .into_iter()
    .map(String::from)
    .collect()
});
// IND
static PRE_SOCIO: Lazy<HashMap<i32, String>> =
    Lazy::new(|| load_mapping("mappings/pre_socio.json"));
static BESKST13: Lazy<HashMap<i32, String>> = Lazy::new(|| load_mapping("mappings/beskst13.json"));

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
        let is_idan_schema = columns_def.iter().any(|col| {
            col.get("name")
                .and_then(|n| n.as_str())
                .map_or(false, |name| name == "JOBKAT")
        });
        let is_ind_schema = columns_def.iter().any(|col| {
            col.get("name")
                .and_then(|n| n.as_str())
                .map_or(false, |name| name == "BESKST13")
        });
        let is_uddf_schema = columns_def.iter().any(|col| {
            col.get("name")
                .and_then(|n| n.as_str())
                .map_or(false, |name| name == "HFAUDD")
        });
        let is_lpr3_diagnoser_schema = columns_def.iter().any(|col| {
            col.get("name")
                .and_then(|n| n.as_str())
                .map_or(false, |name| name == "diagnosekode")
        });
        let is_lpr3_kontakter_schema = columns_def.iter().any(|col| {
            col.get("name")
                .and_then(|n| n.as_str())
                .map_or(false, |name| name == "DW_EK_KONTAKT")
        });
        let is_lpr_adm_schema = columns_def.iter().any(|col| {
            col.get("name")
                .and_then(|n| n.as_str())
                .map_or(false, |name| name == "C_ADIAG")
        });
        let is_lpr_bes_schema = columns_def.iter().any(|col| {
            col.get("name")
                .and_then(|n| n.as_str())
                .map_or(false, |name| name == "D_AMBDTO")
        });
        let is_lpr_diag_schema = columns_def.iter().any(|col| {
            col.get("name")
                .and_then(|n| n.as_str())
                .map_or(false, |name| name == "C_DIAG")
        });

        for col_def in columns_def {
            let col_name = col_def
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_default();

            let series = if is_akm_schema {
                create_akm_series(col_name, no_rows)
            } else if is_idan_schema {
                create_idan_series(col_name, no_rows)
            } else if is_ind_schema {
                create_ind_series(col_name, no_rows)
            } else if is_uddf_schema {
                create_uddf_series(col_name, no_rows)
            } else if is_lpr3_diagnoser_schema {
                create_lpr3_diagnoser_series(col_name, no_rows)
            } else if is_lpr3_kontakter_schema {
                create_lpr3_kontakter_series(col_name, no_rows)
            } else if is_lpr_adm_schema {
                create_lpr_adm_series(col_name, no_rows)
            } else if is_lpr_bes_schema {
                create_lpr_bes_series(col_name, no_rows)
            } else if is_lpr_diag_schema {
                create_lpr_diag_series(col_name, no_rows)
            } else {
                create_bef_series(col_name, no_rows)
            };
            columns.push(series);
        }
    }
    Ok(DataFrame::new(columns)?)
}

fn create_lpr_diag_series(col_name: &str, no_rows: usize) -> Series {
    let col_name = PlSmallStr::from(col_name);

    match col_name.as_str() {
        "C_DIAG" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let letter = (b'A' + rand::thread_rng().gen_range(0..26)) as char;
                    let number: u16 = rand::thread_rng().gen_range(0..100);
                    format!("{}{:02}", letter, number)
                })
                .collect();
            Series::new(col_name, data)
        }
        "C_DIAGTYPE" => {
            let types = ["A", "B", "H", "M", "G"];
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| types.choose(&mut rand::thread_rng()).unwrap().to_string())
                .collect();
            Series::new(col_name, data)
        }
        "C_TILDIAG" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    if rand::thread_rng().gen_bool(0.7) {
                        String::new()
                    } else {
                        let letter = (b'A' + rand::thread_rng().gen_range(0..26)) as char;
                        let number: u16 = rand::thread_rng().gen_range(0..100);
                        format!("{}{:02}", letter, number)
                    }
                })
                .collect();
            Series::new(col_name, data)
        }
        "LEVERANCEDATO" => {
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let year = rand::thread_rng().gen_range(2000..2023);
                    let month = rand::thread_rng().gen_range(1..13);
                    let day = rand::thread_rng().gen_range(1..29);
                    let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
                    (date.year() * 10000 + date.month() as i32 * 100 + date.day() as i32) as i32
                })
                .collect();
            Series::new(col_name, data)
        }
        "RECNUM" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    format!(
                        "{:020}",
                        rand::thread_rng().gen_range(1_u64..1_000_000_000_000_000_000_u64)
                    )
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
        _ => panic!("Unsupported LPR_DIAG column: {}", col_name),
    }
}

fn create_lpr_bes_series(col_name: &str, no_rows: usize) -> Series {
    let col_name = PlSmallStr::from(col_name);

    match col_name.as_str() {
        "D_AMBDTO" | "LEVERANCEDATO" => {
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let year = rand::thread_rng().gen_range(2000..2023);
                    let month = rand::thread_rng().gen_range(1..13);
                    let day = rand::thread_rng().gen_range(1..29);
                    let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
                    (date.year() * 10000 + date.month() as i32 * 100 + date.day() as i32) as i32
                })
                .collect();
            Series::new(col_name, data)
        }
        "RECNUM" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    format!(
                        "{:020}",
                        rand::thread_rng().gen_range(1_u64..1_000_000_000_000_000_000_u64)
                    )
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
        _ => panic!("Unsupported LPR_BES column: {}", col_name),
    }
}

fn create_lpr_adm_series(col_name: &str, no_rows: usize) -> Series {
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
        "C_ADIAG" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let letter = (b'A' + rand::thread_rng().gen_range(0..26)) as char;
                    let number: u16 = rand::thread_rng().gen_range(0..100);
                    format!("{}{:02}", letter, number)
                })
                .collect();
            Series::new(col_name, data)
        }
        "C_AFD" | "C_HAFD" | "K_AFD" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| format!("{:04}", rand::thread_rng().gen_range(1000..9999)))
                .collect();
            Series::new(col_name, data)
        }
        "C_HENM" | "C_INDM" | "C_KONTAARS" | "C_UDM" => {
            let codes = ["A", "B", "C", "D", "E"];
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| codes.choose(&mut rand::thread_rng()).unwrap().to_string())
                .collect();
            Series::new(col_name, data)
        }
        "C_HSGH" | "C_SGH" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| format!("{:04}", rand::thread_rng().gen_range(1000..9999)))
                .collect();
            Series::new(col_name, data)
        }
        "C_KOM" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| format!("{:03}", rand::thread_rng().gen_range(100..999)))
                .collect();
            Series::new(col_name, data)
        }
        "C_PATTYPE" => {
            let types = ["0", "1", "2", "3"];
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| types.choose(&mut rand::thread_rng()).unwrap().to_string())
                .collect();
            Series::new(col_name, data)
        }
        "C_SPEC" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| format!("{:03}", rand::thread_rng().gen_range(1..100)))
                .collect();
            Series::new(col_name, data)
        }
        "CPRTJEK" | "CPRTYPE" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    if rand::thread_rng().gen_bool(0.5) {
                        "V"
                    } else {
                        "U"
                    }
                    .to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "D_HENDTO" | "D_INDDTO" | "D_UDDTO" => {
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let year = rand::thread_rng().gen_range(2000..2023);
                    let month = rand::thread_rng().gen_range(1..13);
                    let day = rand::thread_rng().gen_range(1..29);
                    let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
                    (date.year() * 10000 + date.month() as i32 * 100 + date.day() as i32) as i32
                })
                .collect();
            Series::new(col_name, data)
        }
        "RECNUM" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    format!(
                        "{:020}",
                        rand::thread_rng().gen_range(1_u64..1_000_000_000_000_000_000_u64)
                    )
                })
                .collect();
            Series::new(col_name, data)
        }
        "V_ALDDG" => {
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(0..36500))
                .collect();
            Series::new(col_name, data)
        }
        "V_ALDER" => {
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(0..100))
                .collect();
            Series::new(col_name, data)
        }
        "V_INDMINUT" => {
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(0..60))
                .collect();
            Series::new(col_name, data)
        }
        "V_INDTIME" | "V_UDTIME" => {
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(0..24))
                .collect();
            Series::new(col_name, data)
        }
        "V_SENGDAGE" => {
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(0..100))
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
        _ => panic!("Unsupported LPR_ADM column: {}", col_name),
    }
}

fn create_lpr3_kontakter_series(col_name: &str, no_rows: usize) -> Series {
    let col_name = PlSmallStr::from(col_name);

    match col_name.as_str() {
        "SORENHED_IND" | "SORENHED_HEN" | "SORENHED_ANS" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| format!("{:06}", rand::thread_rng().gen_range(100000..999999)))
                .collect();
            Series::new(col_name, data)
        }
        "DW_EK_KONTAKT" | "DW_EK_FORLOEB" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    format!(
                        "{:020}",
                        rand::thread_rng().gen_range(1_u64..1_000_000_000_000_000_000_u64)
                    )
                })
                .collect();
            Series::new(col_name, data)
        }
        "CPR" => {
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
        "dato_start" | "dato_slut" | "dato_behandling_start" | "dato_indberetning" => {
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let year = rand::thread_rng().gen_range(2000..2023);
                    let month = rand::thread_rng().gen_range(1..13);
                    let day = rand::thread_rng().gen_range(1..29);
                    let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
                    (date.year() * 10000 + date.month() as i32 * 100 + date.day() as i32) as i32
                })
                .collect();
            Series::new(col_name, data)
        }
        "tidspunkt_start" | "tidspunkt_slut" | "tidspunkt_behandling_start" => {
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let hour = rand::thread_rng().gen_range(0..24);
                    let minute = rand::thread_rng().gen_range(0..60);
                    let second = rand::thread_rng().gen_range(0..60);
                    let time = NaiveTime::from_hms_opt(hour, minute, second).unwrap();
                    (time.hour() * 3600 + time.minute() * 60 + time.second()) as i32
                })
                .collect();
            Series::new(col_name, data)
        }
        "aktionsdiagnose" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let letter = (b'A' + rand::thread_rng().gen_range(0..26)) as char;
                    let number: u16 = rand::thread_rng().gen_range(0..100);
                    format!("{}{:02}", letter, number)
                })
                .collect();
            Series::new(col_name, data)
        }
        "kontaktaarsag" => {
            let aarsager = ["ALCA00", "ALCA10", "ALCA20", "ALCA30", "ALCA40"];
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    aarsager
                        .choose(&mut rand::thread_rng())
                        .unwrap()
                        .to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "prioritet" => {
            let prioriteter = ["ATA1", "ATA2", "ATA3"];
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    prioriteter
                        .choose(&mut rand::thread_rng())
                        .unwrap()
                        .to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "kontakttype" => {
            let typer = ["ALCA00", "ALCA10", "ALCA20", "ALCA30", "ALCA40"];
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| typer.choose(&mut rand::thread_rng()).unwrap().to_string())
                .collect();
            Series::new(col_name, data)
        }
        "henvisningsaarsag" | "henvisningsmaade" => {
            let aarsager = ["ALCA00", "ALCA10", "ALCA20", "ALCA30", "ALCA40"];
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    aarsager
                        .choose(&mut rand::thread_rng())
                        .unwrap()
                        .to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "lprindberetningssytem" => {
            let systems = ["PAS", "OPUS", "COSMIC", "EPJ", "MidtEPJ"];
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| systems.choose(&mut rand::thread_rng()).unwrap().to_string())
                .collect();
            Series::new(col_name, data)
        }
        _ => panic!("Unsupported LPR3_KONTAKTER column: {}", col_name),
    }
}

fn create_lpr3_diagnoser_series(col_name: &str, no_rows: usize) -> Series {
    let col_name = PlSmallStr::from(col_name);

    match col_name.as_str() {
        "DW_EK_KONTAKT" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    format!(
                        "{:020}",
                        rand::thread_rng().gen_range(1_u64..1_000_000_000_000_000_000_u64)
                    )
                })
                .collect();
            Series::new(col_name, data)
        }
        "diagnosekode" | "diagnosekode_parent" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let letter = (b'A' + rand::thread_rng().gen_range(0..26)) as char;
                    let number: u16 = rand::thread_rng().gen_range(0..100);
                    format!("{}{:02}", letter, number)
                })
                .collect();
            Series::new(col_name, data)
        }
        "diagnosetype" | "diagnosetype_parent" => {
            let types = ["A", "B", "H", "M", "G"];
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| types.choose(&mut rand::thread_rng()).unwrap().to_string())
                .collect();
            Series::new(col_name, data)
        }
        "senere_afkraeftet" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    if rand::thread_rng().gen_bool(0.1) {
                        "1"
                    } else {
                        "0"
                    }
                    .to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "lprindberetningssystem" => {
            let systems = ["LPR3", "OPUS", "COSMIC", "EPJ", "MidtEPJ"];
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| systems.choose(&mut rand::thread_rng()).unwrap().to_string())
                .collect();
            Series::new(col_name, data)
        }
        _ => panic!("Unsupported LPR3_DIAGNOSER column: {}", col_name),
    }
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

fn create_uddf_series(col_name: &str, no_rows: usize) -> Series {
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
        "CPRTJEK" | "CPRTYPE" => {
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
        "HFAUDD" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let isced_level = rand::thread_rng().gen_range(1..=9);
                    format!("{}", isced_level)
                })
                .collect();
            Series::new(col_name, data)
        }
        "HF_KILDE" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    ["A", "B", "C", "D", "E"]
                        .choose(&mut rand::thread_rng())
                        .unwrap()
                        .to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "HF_VFRA" | "HF_VTIL" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let year = rand::thread_rng().gen_range(1900..2023);
                    let month = rand::thread_rng().gen_range(1..13);
                    let day = rand::thread_rng().gen_range(1..29);
                    NaiveDate::from_ymd_opt(year, month, day)
                        .unwrap()
                        .format("%Y%m%d")
                        .to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "INSTNR" => {
            let data: Vec<i8> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(1..100))
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
        _ => panic!("Unsupported UDDF column: {}", col_name),
    }
}

fn create_ind_series(col_name: &str, no_rows: usize) -> Series {
    let col_name = PlSmallStr::from(col_name);

    match col_name.as_str() {
        "BESKST13" => {
            let keys: Vec<i32> = BESKST13.keys().cloned().collect();
            let data: Vec<i32> = (0..no_rows)
                .into_par_iter()
                .map(|_| *keys.choose(&mut rand::thread_rng()).unwrap())
                .collect();
            Series::new(col_name, data)
        }
        "CPRTJEK" | "CPRTYPE" => {
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
        "LOENMV_13" => {
            let data: Vec<f64> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(0.0..1_000_000.0))
                .collect();
            Series::new(col_name, data)
        }
        "PERINDKIALT_13" => {
            let data: Vec<f64> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(0.0..2_000_000.0))
                .collect();
            Series::new(col_name, data)
        }
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
        "PRE_SOCIO" => {
            let keys: Vec<i32> = PRE_SOCIO.keys().cloned().collect();
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
        _ => panic!("Unsupported IND column: {}", col_name),
    }
}

fn create_idan_series(col_name: &str, no_rows: usize) -> Series {
    let col_name = PlSmallStr::from(col_name);

    match col_name.as_str() {
        "ARBGNR" | "ARBNR" | "CVRNR" | "LBNR" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    format!(
                        "{:08}",
                        rand::thread_rng().gen_range(10000000_u32..99999999_u32)
                    )
                })
                .collect();
            Series::new(col_name, data)
        }
        "CPRTJEK" | "CPRTYPE" => {
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
        "JOBKAT" => {
            let keys: Vec<i8> = JOBKAT.keys().cloned().collect();
            let data: Vec<i8> = (0..no_rows)
                .into_par_iter()
                .map(|_| *keys.choose(&mut rand::thread_rng()).unwrap())
                .collect();
            Series::new(col_name, data)
        }
        "JOBLON" => {
            let data: Vec<f64> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(15000.0..100000.0))
                .collect();
            Series::new(col_name, data)
        }
        "STILL" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| STILL.choose(&mut rand::thread_rng()).unwrap().clone())
                .collect();
            Series::new(col_name, data)
        }
        "TILKNYT" => {
            let keys: Vec<i8> = TILKNYT.keys().cloned().collect();
            let data: Vec<i8> = (0..no_rows)
                .into_par_iter()
                .map(|_| *keys.choose(&mut rand::thread_rng()).unwrap())
                .collect();
            Series::new(col_name, data)
        }
        _ => panic!("Unsupported IDAN column: {}", col_name),
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

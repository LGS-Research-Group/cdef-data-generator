use crate::generate::mappings::SCD;
use chrono::NaiveDate;
use polars::prelude::*;
use rand::seq::SliceRandom;
use rand::Rng;
use rayon::prelude::*;

fn get_random_diagnosis() -> String {
    let mut rng = rand::thread_rng();
    if rng.gen_bool(0.9) {
        // 90% chance of using original method
        let letter = (b'A' + rng.gen_range(0..26)) as char;
        let number: u16 = rng.gen_range(0..100);
        format!("{}{:02}", letter, number)
    } else {
        // 10% chance of using SCD mapping
        SCD.keys()
            .collect::<Vec<_>>()
            .choose(&mut rng)
            .unwrap()
            .to_string()
    }
}

pub fn create_lpr_diag_series(col_name: &str, no_rows: usize) -> Series {
    let col_name = PlSmallStr::from(col_name);

    match col_name.as_str() {
        "C_DIAG" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| get_random_diagnosis())
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
                .map(|_| get_random_diagnosis())
                .collect();
            Series::new(col_name, data)
        }
        "LEVERANCEDATO" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let year = rand::thread_rng().gen_range(2000..2023);
                    let month = rand::thread_rng().gen_range(1..13);
                    let day = rand::thread_rng().gen_range(1..29);
                    let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
                    date.format("%Y-%m-%d").to_string()
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

pub fn create_lpr_bes_series(col_name: &str, no_rows: usize) -> Series {
    let col_name = PlSmallStr::from(col_name);

    match col_name.as_str() {
        "D_AMBDTO" | "LEVERANCEDATO" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let year = rand::thread_rng().gen_range(2000..2023);
                    let month = rand::thread_rng().gen_range(1..13);
                    let day = rand::thread_rng().gen_range(1..29);
                    let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
                    date.format("%Y-%m-%d").to_string()
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

pub fn create_lpr_adm_series(col_name: &str, no_rows: usize) -> Series {
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
                .map(|_| get_random_diagnosis())
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
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let year = rand::thread_rng().gen_range(2000..2023);
                    let month = rand::thread_rng().gen_range(1..13);
                    let day = rand::thread_rng().gen_range(1..29);
                    let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
                    date.format("%Y-%m-%d").to_string()
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

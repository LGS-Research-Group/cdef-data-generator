use crate::generate::mappings::{
    BESKST13, CIVST, FM_MARK, HUSTYPE, JOBKAT, PLADS, PRE_SOCIO, REG, SOCIO13, STATSB, STILL,
    TILKNYT,
};
use chrono::NaiveDate;
use polars::prelude::*;
use rand::seq::SliceRandom;
use rand::Rng;
use rayon::prelude::*;

pub fn create_akm_series(col_name: &str, no_rows: usize) -> Series {
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

pub fn create_uddf_series(col_name: &str, no_rows: usize) -> Series {
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

pub fn create_ind_series(col_name: &str, no_rows: usize) -> Series {
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

pub fn create_idan_series(col_name: &str, no_rows: usize) -> Series {
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

pub fn create_bef_series(col_name: &str, no_rows: usize) -> Series {
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
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let year = rand::thread_rng().gen_range(1900..2023);
                    let month = rand::thread_rng().gen_range(1..13);
                    let day = rand::thread_rng().gen_range(1..29);
                    format!("{:04}-{:02}-{:02}", year, month, day)
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

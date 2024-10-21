use crate::generate::mappings::{CIVST, FM_MARK, HUSTYPE, PLADS, REG, STATSB};
use crate::generate::pnr::{get_parents_pnr, get_pnr_for_birth_date};
use chrono::{Datelike, NaiveDate};
use polars::prelude::*;
use rand::seq::SliceRandom;
use rand::Rng;
use rayon::prelude::*;

fn generate_birth_date_and_pnr(year: i32) -> (NaiveDate, String) {
    let mut rng = rand::thread_rng();
    let birth_year = rng.gen_range(year - 100..=year);
    let birth_month = rng.gen_range(1..=12);
    let birth_day = rng.gen_range(1..=28);
    let birth_date = NaiveDate::from_ymd_opt(birth_year, birth_month, birth_day).unwrap();
    let pnr = get_pnr_for_birth_date(birth_date);
    (birth_date, pnr)
}

pub fn create_bef_series(col_name: &str, no_rows: usize, year: i32) -> Series {
    let col_name = PlSmallStr::from(col_name);

    match col_name.as_str() {
        "PNR" | "FOED_DAG" | "ALDER" => {
            let data: Vec<(NaiveDate, String)> = (0..no_rows)
                .into_par_iter()
                .map(|_| generate_birth_date_and_pnr(year))
                .collect();

            match col_name.as_str() {
                "PNR" => Series::new(
                    col_name,
                    data.iter()
                        .map(|(_, pnr)| pnr.clone())
                        .collect::<Vec<String>>(),
                ),
                "FOED_DAG" => Series::new(
                    col_name,
                    data.iter()
                        .map(|(date, _)| date.format("%Y-%m-%d").to_string())
                        .collect::<Vec<String>>(),
                ),
                "ALDER" => Series::new(
                    col_name,
                    data.iter()
                        .map(|(date, _)| year - date.year())
                        .collect::<Vec<i32>>(),
                ),
                _ => unreachable!(),
            }
        }
        "FAR_ID" | "MOR_ID" => {
            let pnrs = create_bef_series("PNR", no_rows, year);
            let data: Vec<Option<String>> = pnrs
                .str()
                .unwrap()
                .into_iter()
                .map(|pnr| {
                    let (mother, father) = get_parents_pnr(pnr.unwrap());
                    if col_name.as_str() == "FAR_ID" {
                        father
                    } else {
                        mother
                    }
                })
                .collect();
            Series::new(col_name, data)
        }
        "KOEN" => {
            let pnrs = create_bef_series("PNR", no_rows, year);
            let data: Vec<String> = pnrs
                .str()
                .unwrap()
                .into_iter()
                .map(|pnr| {
                    let last_digit = pnr.unwrap().chars().last().unwrap().to_digit(10).unwrap();
                    if last_digit % 2 == 0 { "K" } else { "M" }.to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "CIVST" => {
            let ages = create_bef_series("ALDER", no_rows, year);
            let data: Vec<String> = ages
                .i32()
                .unwrap()
                .into_iter()
                .map(|age| {
                    let mut rng = rand::thread_rng();
                    let civst_key = match age.unwrap() {
                        0..=17 => "U",
                        18..=24 => {
                            if rng.gen_bool(0.8) {
                                "U"
                            } else {
                                "G"
                            }
                        }
                        _ => ["U", "G", "F", "E"].choose(&mut rng).unwrap(),
                    };
                    CIVST
                        .get(civst_key)
                        .unwrap_or(&civst_key.to_string())
                        .clone()
                })
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
        "KOM" => {
            let data: Vec<i16> = (0..no_rows)
                .into_par_iter()
                .map(|_| rand::thread_rng().gen_range(101..851))
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
        "AEGTE_ID" => {
            let pnrs = create_bef_series("PNR", no_rows, year);
            let ages = create_bef_series("ALDER", no_rows, year);

            let data: Vec<Option<String>> = pnrs
                .str()
                .unwrap()
                .into_iter()
                .zip(ages.i32().unwrap().into_iter())
                .map(|(pnr, age)| {
                    let _pnr = pnr.unwrap();
                    let age = age.unwrap();

                    let spouse_probability = match age {
                        0..=17 => 0.0,  // No spouse for minors
                        18..=25 => 0.1, // Low probability for young adults
                        26..=35 => 0.5, // Higher probability for adults
                        36..=60 => 0.7, // Highest probability for middle-aged
                        _ => 0.6,       // Slightly lower for seniors
                    };

                    if rand::thread_rng().gen_bool(spouse_probability) {
                        let spouse_birth_year = year - age - rand::thread_rng().gen_range(-5..=5);
                        let spouse_birth_date = NaiveDate::from_ymd_opt(
                            spouse_birth_year,
                            rand::thread_rng().gen_range(1..=12),
                            rand::thread_rng().gen_range(1..=28),
                        )
                        .unwrap();
                        Some(get_pnr_for_birth_date(spouse_birth_date))
                    } else {
                        None
                    }
                })
                .collect();

            Series::new(col_name, data)
        }
        _ => panic!("Unsupported column: {}", col_name),
    }
}

use crate::generate::pnr::get_pnr_for_birth_date;
use crate::generate::recnum::generate_recnum;
use crate::generate::recnum::get_recnum_for_pnr;
use crate::generate::utils::generate_date_for_year;
use crate::generate::utils::get_random_diagnosis;
use crate::registers::bef::create_bef_series;
use chrono::{NaiveDate, NaiveTime};
use polars::prelude::*;
use rand::seq::SliceRandom;
use rand::Rng;
use rayon::prelude::*;

pub fn create_lpr3_kontakter_series(col_name: &str, no_rows: usize, year: i32) -> Series {
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
            let pnrs = create_bef_series("PNR", no_rows, year);
            let data: Vec<String> = pnrs
                .str()
                .unwrap()
                .into_iter()
                .map(|pnr| get_recnum_for_pnr(pnr.unwrap(), year))
                .collect();
            Series::new(col_name, data)
        }
        "CPR" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let birth_date = NaiveDate::from_ymd_opt(
                        year - rand::thread_rng().gen_range(0..100),
                        rand::thread_rng().gen_range(1..=12),
                        rand::thread_rng().gen_range(1..=28),
                    )
                    .unwrap();
                    get_pnr_for_birth_date(birth_date)
                })
                .collect();
            Series::new(col_name, data)
        }
        "dato_start" | "dato_slut" | "dato_behandling_start" | "dato_indberetning" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let date = generate_date_for_year(year);
                    NaiveDate::parse_from_str(&date, "%Y-%m-%d")
                        .unwrap()
                        .format("%d%b%Y")
                        .to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "tidspunkt_start" | "tidspunkt_slut" | "tidspunkt_behandling_start" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| {
                    let hour = rand::thread_rng().gen_range(0..24);
                    let minute = rand::thread_rng().gen_range(0..60);
                    let second = rand::thread_rng().gen_range(0..60);
                    let time = NaiveTime::from_hms_opt(hour, minute, second).unwrap();
                    time.format("%H:%M:%S").to_string()
                })
                .collect();
            Series::new(col_name, data)
        }
        "aktionsdiagnose" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| get_random_diagnosis())
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

pub fn create_lpr3_diagnoser_series(col_name: &str, no_rows: usize) -> Series {
    let col_name = PlSmallStr::from(col_name);

    match col_name.as_str() {
        "DW_EK_KONTAKT" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| generate_recnum())
                .collect();
            Series::new(col_name, data)
        }
        "diagnosekode" | "diagnosekode_parent" => {
            let data: Vec<String> = (0..no_rows)
                .into_par_iter()
                .map(|_| get_random_diagnosis())
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

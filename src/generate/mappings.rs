use crate::generate::utils::load_mapping;
use once_cell::sync::Lazy;
use std::collections::HashMap;

// AKM
pub static SOCIO13: Lazy<HashMap<i32, String>> =
    Lazy::new(|| load_mapping("mappings/socio13.json"));
// BEF
pub static CIVST: Lazy<HashMap<String, String>> = Lazy::new(|| load_mapping("mappings/civst.json"));
pub static FM_MARK: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/fm_mark.json"));
pub static HUSTYPE: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/hustype.json"));
pub static PLADS: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/plads.json"));
pub static REG: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/reg.json"));
pub static STATSB: Lazy<HashMap<i32, String>> = Lazy::new(|| load_mapping("mappings/statsb.json"));
// IDAN
pub static JOBKAT: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/jobkat.json"));
pub static TILKNYT: Lazy<HashMap<i8, String>> = Lazy::new(|| load_mapping("mappings/tilknyt.json"));
pub static STILL: Lazy<Vec<String>> = Lazy::new(|| {
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
pub static PRE_SOCIO: Lazy<HashMap<i32, String>> =
    Lazy::new(|| load_mapping("mappings/pre_socio.json"));
pub static BESKST13: Lazy<HashMap<i32, String>> =
    Lazy::new(|| load_mapping("mappings/beskst13.json"));

//pub static ICD10: Lazy<HashMap<String, String>> = Lazy::new(|| load_mapping("mappings/icd10.json"));
pub static SCD: Lazy<HashMap<String, String>> = Lazy::new(|| load_mapping("mappings/scd.json"));

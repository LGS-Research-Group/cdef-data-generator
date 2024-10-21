use chrono::NaiveDate;
use once_cell::sync::Lazy;
use rand::seq::IteratorRandom;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

static RECNUM_POOL: Lazy<Mutex<RecnumPool>> = Lazy::new(|| Mutex::new(RecnumPool::new()));

struct Contact {
    recnum: String,
    pnr: String,
    contact_date: NaiveDate,
}

struct RecnumPool {
    contacts: HashMap<String, Contact>,
    pnr_to_recnums: HashMap<String, HashSet<String>>,
    next_recnum: u64,
}

impl RecnumPool {
    fn new() -> Self {
        RecnumPool {
            contacts: HashMap::new(),
            pnr_to_recnums: HashMap::new(),
            next_recnum: 1,
        }
    }

    fn generate_recnum(&mut self) -> String {
        let recnum = format!("{:020}", self.next_recnum);
        self.next_recnum += 1;
        recnum
    }

    fn add_contact(&mut self, pnr: &str, year: i32) -> String {
        let mut rng = rand::thread_rng();
        let contact_date =
            NaiveDate::from_ymd_opt(year, rng.gen_range(1..13), rng.gen_range(1..29)).unwrap();

        let recnum = self.generate_recnum();

        let contact = Contact {
            recnum: recnum.clone(),
            pnr: pnr.to_string(),
            contact_date,
        };

        self.contacts.insert(recnum.clone(), contact);
        self.pnr_to_recnums
            .entry(pnr.to_string())
            .or_insert_with(HashSet::new)
            .insert(recnum.clone());

        recnum
    }

    fn get_random_recnum_for_pnr(&self, pnr: &str) -> Option<String> {
        self.pnr_to_recnums
            .get(pnr)
            .and_then(|recnums| recnums.iter().choose(&mut rand::thread_rng()).cloned())
    }

    fn get_or_create_recnum_for_pnr(&mut self, pnr: &str, year: i32) -> String {
        if let Some(recnum) = self.get_random_recnum_for_pnr(pnr) {
            recnum
        } else {
            self.add_contact(pnr, year)
        }
    }
}

pub fn get_recnum_for_pnr(pnr: &str, year: i32) -> String {
    let mut pool = RECNUM_POOL.lock().unwrap();
    pool.get_or_create_recnum_for_pnr(pnr, year)
}

pub fn get_random_recnum() -> String {
    let pool = RECNUM_POOL.lock().unwrap();
    pool.contacts
        .keys()
        .choose(&mut rand::thread_rng())
        .cloned()
        .unwrap()
}

pub fn generate_recnum() -> String {
    let mut pool = RECNUM_POOL.lock().unwrap();
    pool.generate_recnum()
}

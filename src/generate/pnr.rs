use chrono::{Datelike, NaiveDate};
use once_cell::sync::Lazy;
use rand::seq::IteratorRandom;
use rand::seq::SliceRandom;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Mutex;

static PNR_POOL: Lazy<Mutex<PnrPool>> = Lazy::new(|| Mutex::new(PnrPool::new()));

#[derive(Clone)]
struct Person {
    pnr: String,
    birth_date: NaiveDate,
    gender: char,
    mother_pnr: Option<String>,
    father_pnr: Option<String>,
}

struct PnrPool {
    persons: HashMap<String, Person>,
    years: Vec<i32>,
    min_parent_age: i32,
    max_parent_age: i32,
}

impl PnrPool {
    fn new() -> Self {
        PnrPool {
            persons: HashMap::new(),
            years: Vec::new(),
            min_parent_age: 18,
            max_parent_age: 50,
        }
    }

    fn generate_pnr(&self, birth_date: NaiveDate, gender: char) -> String {
        let mut rng = rand::thread_rng();
        let day = birth_date.day();
        let month = birth_date.month();
        let year = birth_date.year() % 100;
        let century = birth_date.year() / 100;

        let seventh_digit = match century {
            18 => rng.gen_range(5..8),
            19 => {
                if birth_date.year() < 1937 {
                    rng.gen_range(0..4)
                } else {
                    rng.gen_range(4..10)
                }
            }
            20 => rng.gen_range(0..4),
            _ => rng.gen_range(4..10),
        };

        let last_three_digits = loop {
            let digits = rng.gen_range(0..999);
            if (gender == 'M' && digits % 2 == 1) || (gender == 'K' && digits % 2 == 0) {
                break digits;
            }
        };

        format!(
            "{:02}{:02}{:02}-{}{:03}",
            day, month, year, seventh_digit, last_three_digits
        )
    }

    fn add_person(
        &mut self,
        birth_year: i32,
        mother_pnr: Option<String>,
        father_pnr: Option<String>,
    ) -> String {
        let mut rng = rand::thread_rng();
        let birth_date =
            NaiveDate::from_ymd_opt(birth_year, rng.gen_range(1..13), rng.gen_range(1..29))
                .unwrap();
        let gender = if rng.gen_bool(0.5) { 'M' } else { 'K' };

        let pnr = self.generate_pnr(birth_date, gender);

        let person = Person {
            pnr: pnr.clone(),
            birth_date,
            gender,
            mother_pnr,
            father_pnr,
        };

        self.persons.insert(pnr.clone(), person);
        pnr
    }

    fn generate_year(&mut self, year: i32) {
        if self.years.contains(&year) {
            return;
        }

        let mut rng = rand::thread_rng();
        let num_births = rng.gen_range(55000..65001);

        // Generate parents first
        for _ in 0..num_births * 2 {
            let parent_birth_year = year - rng.gen_range(self.min_parent_age..=self.max_parent_age);
            self.add_person(parent_birth_year, None, None);
        }

        // Then generate children with parents
        for _ in 0..num_births {
            let mother_pnr = self.get_random_adult_pnr('K', year);
            let father_pnr = self.get_random_adult_pnr('M', year);
            self.add_person(year, mother_pnr, father_pnr);
        }

        self.years.push(year);
    }

    fn get_random_adult_pnr(&self, gender: char, current_year: i32) -> Option<String> {
        let adults: Vec<&Person> = self
            .persons
            .values()
            .filter(|p| {
                p.gender == gender
                    && (current_year - p.birth_date.year()) >= self.min_parent_age
                    && (current_year - p.birth_date.year()) <= self.max_parent_age
            })
            .collect();

        adults
            .choose(&mut rand::thread_rng())
            .map(|p| p.pnr.clone())
    }

    fn get_random_pnr(&self) -> Option<String> {
        self.persons.keys().choose(&mut rand::thread_rng()).cloned()
    }

    fn get_or_create_pnr(&mut self, birth_date: NaiveDate) -> String {
        let year = birth_date.year();
        self.generate_year(year);

        let gender = if rand::thread_rng().gen_bool(0.5) {
            'M'
        } else {
            'K'
        };
        let pnr = self.generate_pnr(birth_date, gender);

        if !self.persons.contains_key(&pnr) {
            let person = Person {
                pnr: pnr.clone(),
                birth_date,
                gender,
                mother_pnr: None,
                father_pnr: None,
            };
            self.persons.insert(pnr.clone(), person);
        }

        pnr
    }
}

// Function to ensure a year is generated and return a random PNR
pub fn get_pnr_for_birth_date(birth_date: NaiveDate) -> String {
    let mut pool = PNR_POOL.lock().unwrap();
    pool.get_or_create_pnr(birth_date)
}

// Function to get parents' PNRs
pub fn get_parents_pnr(pnr: &str) -> (Option<String>, Option<String>) {
    let mut pool = PNR_POOL.lock().unwrap();
    if let Some(person) = pool.persons.get(pnr).cloned() {
        let mother_pnr = person.mother_pnr.clone().or_else(|| {
            let mother_birth_year = person.birth_date.year()
                - rand::thread_rng().gen_range(pool.min_parent_age..=pool.max_parent_age);
            Some(pool.add_person(mother_birth_year, None, None))
        });
        let father_pnr = person.father_pnr.clone().or_else(|| {
            let father_birth_year = person.birth_date.year()
                - rand::thread_rng().gen_range(pool.min_parent_age..=pool.max_parent_age);
            Some(pool.add_person(father_birth_year, None, None))
        });
        (mother_pnr, father_pnr)
    } else {
        (None, None)
    }
}

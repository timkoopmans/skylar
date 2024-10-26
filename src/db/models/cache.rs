use std::sync::atomic::{AtomicUsize, Ordering};
use crate::db::models::{ReadPayload, WritePayload};
use once_cell::sync::Lazy;
use rand::distributions::Distribution;
use rand::distributions::{WeightedIndex};
use rand::prelude::SliceRandom;
use rand::Rng;
use rand_distr::{Binomial, Geometric, Normal, Poisson, Zipf};
use scylla::{FromRow, SerializeRow};
use uuid::Uuid;

pub const DDL_CACHE: &str = r#"
    CREATE KEYSPACE IF NOT EXISTS skylar WITH replication =
    {'class': 'NetworkTopologyStrategy', 'replication_factor': <RF>};

    USE skylar;
    CREATE TABLE IF NOT EXISTS skylar.cache
    (
        device_id   uuid PRIMARY KEY,
        temperature int
    )
"#;

pub const INSERT_KEY_VALUE: &str = "
    INSERT INTO skylar.cache
    (
        device_id,
        temperature
    )
    VALUES (?, ?)
";

pub const SELECT_KEY_VALUE: &str = "
    SELECT
        device_id,
        temperature
    FROM skylar.cache
    WHERE device_id = ?
";

static SEQUENTIAL_INDEX_A: AtomicUsize = AtomicUsize::new(0);

static DEVICES: Lazy<Vec<Uuid>> = Lazy::new(|| {
    let size = 1000000;
    (0..size).map(|_| Uuid::new_v4()).collect()
});

static WEIGHTS_NORMAL: Lazy<WeightedIndex<usize>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let normal = Normal::new(DEVICES.len() as f64 / 2.0, DEVICES.len() as f64 / 6.0)
        .expect("Failed to create normal distribution");
    let mut weights = vec![0; DEVICES.len()];
    for weight in weights.iter_mut() {
        let sample = normal.sample(&mut rng).round() as usize;
        if sample < DEVICES.len() {
            *weight += 1;
        }
    }
    WeightedIndex::new(weights).unwrap()
});

static WEIGHTS_POISSON: Lazy<WeightedIndex<usize>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let poisson =
        Poisson::new(DEVICES.len() as f64 / 2.0).expect("Failed to create poisson distribution");
    let mut weights = vec![0; DEVICES.len()];
    for weight in weights.iter_mut() {
        let sample = poisson.sample(&mut rng) as usize;
        if sample < DEVICES.len() {
            *weight += 1;
        }
    }
    WeightedIndex::new(weights).unwrap()
});

static WEIGHTS_BINOMIAL: Lazy<WeightedIndex<usize>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let binomial = Binomial::new(20, 0.3).expect("Failed to create binomial distribution");
    let mut weights = vec![0; DEVICES.len()];
    for weight in weights.iter_mut() {
        let sample = binomial.sample(&mut rng) as usize;
        if sample < DEVICES.len() {
            *weight += 1;
        }
    }
    WeightedIndex::new(weights).unwrap()
});

static WEIGHTS_GEOMETRIC: Lazy<WeightedIndex<usize>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let geometric = Geometric::new(0.3).expect("Failed to create geometric distribution");
    let mut weights = vec![0; DEVICES.len()];
    for weight in weights.iter_mut() {
        let sample = geometric.sample(&mut rng) as usize;
        if sample < DEVICES.len() {
            *weight += 1;
        }
    }
    WeightedIndex::new(weights).unwrap()
});

static WEIGHTS_ZIPF: Lazy<WeightedIndex<usize>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let zipf = Zipf::new(DEVICES.len() as u64, 1.5).expect("Failed to create zipf distribution");
    let mut weights = vec![0; DEVICES.len()];
    for weight in weights.iter_mut() {
        let sample = zipf.sample(&mut rng) as usize;
        if sample < DEVICES.len() {
            *weight += 1;
        }
    }
    WeightedIndex::new(weights).unwrap()
});

pub fn device_id(distribution: &str) -> Uuid {
    let mut rng = rand::thread_rng();
    let dist = match distribution {
        "sequential" => {
            let index = SEQUENTIAL_INDEX_A.fetch_add(1, Ordering::SeqCst) % DEVICES.len();
            return DEVICES[index];
        }
        "uniform" => return *DEVICES.choose(&mut rng).unwrap(),
        "normal" => &WEIGHTS_NORMAL,
        "poisson" => &WEIGHTS_POISSON,
        "binomial" => &WEIGHTS_BINOMIAL,
        "geometric" => &WEIGHTS_GEOMETRIC,
        "zipf" => &WEIGHTS_ZIPF,
        _ => return *DEVICES.choose(&mut rng).unwrap(),
    };

    DEVICES[dist.sample(&mut rng)]
}

#[derive(Debug, Clone, SerializeRow, FromRow)]
pub struct Cache {
    pub device_id: Uuid,
    pub temperature: i64
}

#[derive(Debug, Clone, SerializeRow, FromRow)]
pub struct CacheValues {
    device_id: Uuid,
}

impl WritePayload for Cache {
    fn insert_query() -> &'static str {
        INSERT_KEY_VALUE
    }

    fn insert_values(distribution: &str) -> Self {
        let mut rng = rand::thread_rng();
        Cache {
            device_id: device_id(distribution),
            temperature: rng.gen_range(0..100)
        }
    }
}

impl ReadPayload for CacheValues {
    fn select_query() -> &'static str {
        SELECT_KEY_VALUE
    }

    fn select_values(_: &str) -> Self {
        CacheValues {
            device_id: Uuid::new_v4(),
        }
    }
}

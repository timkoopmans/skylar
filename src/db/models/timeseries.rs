use crate::db::models::{ReadPayload, WritePayload};
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use rand::distributions::Distribution;
use rand::distributions::{Alphanumeric, DistString, WeightedIndex};
use rand::prelude::SliceRandom;
use rand::Rng;
use rand_distr::{Binomial, Geometric, Normal, Poisson, Zipf};
use scylla::{FromRow, SerializeRow};
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;
static SEQUENTIAL_INDEX_A: AtomicUsize = AtomicUsize::new(0);
static SEQUENTIAL_INDEX_B: AtomicUsize = AtomicUsize::new(0);

pub const DDL_TIMESERIES: &str = r#"
    CREATE KEYSPACE IF NOT EXISTS skylar WITH replication =
    {'class': 'NetworkTopologyStrategy', 'replication_factor': <RF>}
    AND tablets = {'enabled': <TABLETS_ENABLED>, 'initial': <TABLETS>};

    USE skylar;
    CREATE TABLE IF NOT EXISTS skylar.devices
    (
        kind             text,
        link_name        text,
        rack_id          uuid,
        sled_id          uuid,
        sled_model       text,
        sled_revision    int,
        sled_serial      text,
        zone_name        text,
        bytes_sent       int,
        bytes_received   int,
        packets_sent     int,
        packets_received int,
        time             timestamp,
        PRIMARY KEY ((rack_id, sled_id), time)
    )
"#;

static POOL_RACKS: Lazy<Vec<Uuid>> = Lazy::new(|| {
    let size = 3;
    (0..size).map(|_| Uuid::new_v4()).collect()
});

static POOL_SLEDS: Lazy<Vec<Uuid>> = Lazy::new(|| {
    let size = 100000000;
    (0..size).map(|_| Uuid::new_v4()).collect()
});

static WEIGHTS_NORMAL: Lazy<WeightedIndex<usize>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let normal = Normal::new(POOL_SLEDS.len() as f64 / 2.0, POOL_SLEDS.len() as f64 / 6.0)
        .expect("Failed to create normal distribution");
    let mut weights = vec![0; POOL_SLEDS.len()];
    for weight in weights.iter_mut() {
        let sample = normal.sample(&mut rng).round() as usize;
        if sample < POOL_SLEDS.len() {
            *weight += 1;
        }
    }
    WeightedIndex::new(weights).unwrap()
});

static WEIGHTS_POISSON: Lazy<WeightedIndex<usize>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let poisson =
        Poisson::new(POOL_SLEDS.len() as f64 / 2.0).expect("Failed to create poisson distribution");
    let mut weights = vec![0; POOL_SLEDS.len()];
    for weight in weights.iter_mut() {
        let sample = poisson.sample(&mut rng) as usize;
        if sample < POOL_SLEDS.len() {
            *weight += 1;
        }
    }
    WeightedIndex::new(weights).unwrap()
});

static WEIGHTS_BINOMIAL: Lazy<WeightedIndex<usize>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let binomial = Binomial::new(20, 0.3).expect("Failed to create binomial distribution");
    let mut weights = vec![0; POOL_SLEDS.len()];
    for weight in weights.iter_mut() {
        let sample = binomial.sample(&mut rng) as usize;
        if sample < POOL_SLEDS.len() {
            *weight += 1;
        }
    }
    WeightedIndex::new(weights).unwrap()
});

static WEIGHTS_GEOMETRIC: Lazy<WeightedIndex<usize>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let geometric = Geometric::new(0.3).expect("Failed to create geometric distribution");
    let mut weights = vec![0; POOL_SLEDS.len()];
    for weight in weights.iter_mut() {
        let sample = geometric.sample(&mut rng) as usize;
        if sample < POOL_SLEDS.len() {
            *weight += 1;
        }
    }
    WeightedIndex::new(weights).unwrap()
});

static WEIGHTS_ZIPF: Lazy<WeightedIndex<usize>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let zipf = Zipf::new(POOL_SLEDS.len() as u64, 1.5).expect("Failed to create zipf distribution");
    let mut weights = vec![0; POOL_SLEDS.len()];
    for weight in weights.iter_mut() {
        let sample = zipf.sample(&mut rng) as usize;
        if sample < POOL_SLEDS.len() {
            *weight += 1;
        }
    }
    WeightedIndex::new(weights).unwrap()
});

pub fn rack_id(distribution: &str) -> Uuid {
    let mut rng = rand::thread_rng();
    match distribution {
        "sequential" => {
            let index = SEQUENTIAL_INDEX_A.fetch_add(1, Ordering::SeqCst) % POOL_RACKS.len();
            POOL_RACKS[index]
        }
        _ => *POOL_RACKS.choose(&mut rng).unwrap(),
    }
}

pub fn sled_id(distribution: &str) -> Uuid {
    let mut rng = rand::thread_rng();
    let dist = match distribution {
        "sequential" => {
            let index = SEQUENTIAL_INDEX_B.fetch_add(1, Ordering::SeqCst) % POOL_SLEDS.len();
            return POOL_SLEDS[index];
        }
        "uniform" => return *POOL_SLEDS.choose(&mut rng).unwrap(),
        "normal" => &WEIGHTS_NORMAL,
        "poisson" => &WEIGHTS_POISSON,
        "binomial" => &WEIGHTS_BINOMIAL,
        "geometric" => &WEIGHTS_GEOMETRIC,
        "zipf" => &WEIGHTS_ZIPF,
        _ => return *POOL_SLEDS.choose(&mut rng).unwrap(),
    };

    POOL_SLEDS[dist.sample(&mut rng)]
}

pub const INSERT_DEVICE: &str = "
    INSERT INTO skylar.devices
    (
        kind,
        link_name,
        rack_id,
        sled_id,
        sled_model,
        sled_revision,
        sled_serial,
        zone_name,
        bytes_sent,
        bytes_received,
        packets_sent,
        packets_received,
        time
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
";

pub const SELECT_DEVICE: &str = "
    SELECT
        kind,
        link_name,
        rack_id,
        sled_id,
        sled_model,
        sled_revision,
        sled_serial,
        zone_name,
        bytes_sent,
        bytes_received,
        packets_sent,
        packets_received,
        time
    FROM skylar.devices
    WHERE rack_id = ? AND sled_id = ? AND time > ?
";

#[derive(Debug, Clone, SerializeRow, FromRow)]
pub struct Device {
    pub kind: String,
    pub link_name: String,
    pub rack_id: Uuid,
    pub sled_id: Uuid,
    pub sled_model: String,
    pub sled_revision: i32,
    pub sled_serial: String,
    pub zone_name: String,
    pub bytes_sent: i32,
    pub bytes_received: i32,
    pub packets_sent: i32,
    pub packets_received: i32,
    pub time: DateTime<Utc>,
}

#[derive(Debug, Clone, SerializeRow, FromRow)]
pub struct DeviceValues {
    rack_id: Uuid,
    sled_id: Uuid,
    time: DateTime<Utc>,
}

impl WritePayload for Device {
    fn insert_query() -> &'static str {
        INSERT_DEVICE
    }

    fn insert_values(distribution: &str) -> Self {
        let mut rng = rand::thread_rng();
        let now = Utc::now();
        let string = Alphanumeric.sample_string(&mut rand::thread_rng(), 4);
        Device {
            kind: "vnic".to_string(),
            link_name: format!("l-{}", string),
            rack_id: rack_id(distribution),
            sled_id: sled_id(distribution),
            sled_model: format!("m-{}", string),
            sled_revision: rng.gen_range(0..10),
            sled_serial: format!("s-{}", string),
            zone_name: format!("z-{}", string),
            bytes_sent: rng.gen_range(0..1000),
            bytes_received: rng.gen_range(0..1000),
            packets_sent: rng.gen_range(1000..1000000),
            packets_received: rng.gen_range(1000..1000000),
            time: now,
        }
    }
}

impl ReadPayload for DeviceValues {
    fn select_query() -> &'static str {
        SELECT_DEVICE
    }

    fn select_values(distribution: &str) -> Self {
        let time = Utc::now() - chrono::Duration::seconds(5);
        DeviceValues {
            rack_id: rack_id(distribution),
            sled_id: sled_id(distribution),
            time,
        }
    }
}

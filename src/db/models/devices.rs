use crate::db::models::{ReadPayload, WritePayload};
use chrono::{DateTime, Datelike, Utc};
use once_cell::sync::Lazy;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::SliceRandom;
use rand::Rng;
use scylla::{FromRow, SerializeRow};
use uuid::Uuid;

static POOL_RACKS: Lazy<Vec<Uuid>> = Lazy::new(|| {
    let size = 1000;
    (0..size).map(|_| Uuid::new_v4()).collect()
});

static POOL_SLEDS: Lazy<Vec<Uuid>> = Lazy::new(|| {
    let size = 10000;
    (0..size).map(|_| Uuid::new_v4()).collect()
});

pub fn random_rack_id() -> Uuid {
    let mut rng = rand::thread_rng();
    *POOL_RACKS.choose(&mut rng).unwrap()
}

pub fn random_sled_id() -> Uuid {
    let mut rng = rand::thread_rng();
    *POOL_SLEDS.choose(&mut rng).unwrap()
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
        time,
        year,
        month
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        time,
        year,
        month
    FROM skylar.devices
    WHERE year = ? AND month = ? AND rack_id = ? AND sled_id = ?
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
    pub year: i32,
    pub month: i32,
}

#[derive(Debug, Clone, SerializeRow, FromRow)]
pub struct DeviceValues {
    year: i32,
    month: i32,
    rack_id: Uuid,
    sled_id: Uuid,
}

impl WritePayload for Device {
    fn insert_query() -> &'static str {
        INSERT_DEVICE
    }

    fn insert_values() -> Self {
        let mut rng = rand::thread_rng();
        let now = Utc::now();
        let string = Alphanumeric.sample_string(&mut rand::thread_rng(), 4);
        Device {
            kind: "vnic".to_string(),
            link_name: format!("l-{}", string),
            rack_id: Uuid::new_v4(),
            sled_id: Uuid::new_v4(),
            sled_model: format!("m-{}", string),
            sled_revision: rng.gen_range(0..10),
            sled_serial: format!("s-{}", string),
            zone_name: format!("z-{}", string),
            bytes_sent: rng.gen_range(0..1000),
            bytes_received: rng.gen_range(0..1000),
            packets_sent: rng.gen_range(1000..1000000),
            packets_received: rng.gen_range(1000..1000000),
            time: now,
            year: now.year(),
            month: now.month() as i32,
        }
    }
}

impl ReadPayload for DeviceValues {
    fn select_query() -> &'static str {
        SELECT_DEVICE
    }

    fn select_values() -> Self {
        let now = Utc::now();
        DeviceValues {
            rack_id: random_rack_id(),
            sled_id: random_sled_id(),
            year: now.year(),
            month: now.month() as i32,
        }
    }
}

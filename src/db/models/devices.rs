use crate::db::models::{ReadPayload, WritePayload};
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::SliceRandom;
use rand::Rng;
use scylla::{FromRow, SerializeRow};
use uuid::Uuid;

pub const DDL_DEVICES: &str = r#"
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
    let size = 1000000;
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

    fn insert_values() -> Self {
        let mut rng = rand::thread_rng();
        let now = Utc::now();
        let string = Alphanumeric.sample_string(&mut rand::thread_rng(), 4);
        Device {
            kind: "vnic".to_string(),
            link_name: format!("l-{}", string),
            rack_id: random_rack_id(),
            sled_id: random_sled_id(),
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

    fn select_values() -> Self {
        let time = Utc::now() - chrono::Duration::seconds(5);
        DeviceValues {
            rack_id: random_rack_id(),
            sled_id: random_sled_id(),
            time,
        }
    }
}

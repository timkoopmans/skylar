use chrono::{DateTime, Datelike, Utc};
use rand::Rng;
use scylla::SerializeRow;

pub const INSERT_DEVICE: &str = "
    INSERT INTO skylar.devices
    (
        device, temperature, time, year, month
    )
    VALUES (?, ?, ?, ?, ?)
";

#[derive(Debug, Clone, SerializeRow)]
pub struct Device {
    pub device: String,
    pub temperature: i32,
    pub time: DateTime<Utc>,
    pub year: i32,
    pub month: i32,
}

pub fn generate_random_device() -> Device {
    let mut rng = rand::thread_rng();
    let now = Utc::now();
    Device {
        device: format!("device{}", rng.gen::<u32>()),
        temperature: rng.gen_range(0..100),
        time: now,
        year: now.year(),
        month: now.month() as i32,
    }
}

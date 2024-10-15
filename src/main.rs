use crate::db::models::devices::{Device, DeviceValues};
use crate::db::models::users::{User, UserValues};
use anyhow::Result;
use app::App;
use clap::Parser;
use std::sync::Arc;

mod app;
mod db;
mod logging;

#[derive(Debug, Parser, Clone)]
struct Opt {
    /// Host
    #[structopt(long, default_value = "localhost:9042")]
    host: String,

    /// Username
    #[structopt(long, default_value = "cassandra")]
    username: String,

    /// Password
    #[structopt(long, default_value = "cassandra")]
    password: String,

    /// Consistency level
    #[structopt(long, short = 'c', default_value = "LOCAL_QUORUM")]
    consistency_level: String,

    /// Replication factor
    #[structopt(long, short = 'r', default_value = "3")]
    replication_factor: i32,

    /// Datacenter
    #[structopt(long, short = 'd', default_value = "datacenter1")]
    datacenter: String,

    /// Number of tablets, if set to 0 tablets are disabled
    #[structopt(long, short = 't', default_value = "0")]
    tablets: usize,

    /// Number of read threads
    #[structopt(long, short = 'R', default_value = "10")]
    readers: usize,

    /// Number of write threads
    #[structopt(long, short = 'W', default_value = "90")]
    writers: usize,

    /// Payload type
    #[structopt(long, short = 'P', default_value = "devices")]
    payload: String,

    /// Distribution
    /// sequential, uniform, normal, poisson, geometric, binomial, zipf
    #[structopt(long, short = 'D', default_value = "uniform")]
    distribution: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    dotenv::dotenv().ok();
    logging::init();

    let session = db::connection::builder(true, &opt).await?;

    let mut app = App::new();

    match opt.payload.as_str() {
        "devices" => {
            let display = app
                .run::<Device, DeviceValues>(Arc::from(session), &opt)
                .await;
            ratatui::restore();
            display
        }
        "users" => {
            let display = app.run::<User, UserValues>(Arc::from(session), &opt).await;
            ratatui::restore();
            display
        }
        _ => panic!("Unsupported payload type"),
    }
}

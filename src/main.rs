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
    /// Default value: localhost:9042
    #[structopt(long, default_value = "localhost:9042")]
    host: String,

    /// Consistency level
    /// Possible values: ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE
    /// Default value: LOCAL_QUORUM
    #[structopt(long, short = 'c', default_value = "LOCAL_QUORUM")]
    consistency_level: String,

    /// Replication factor
    /// Default value: 1
    #[structopt(long, short = 'r', default_value = "1")]
    replication_factor: i32,

    /// Datacenter
    /// Default value: datacenter1
    #[structopt(long, short = 'd', default_value = "datacenter1")]
    datacenter: String,

    /// Number of read threads
    #[structopt(long, default_value = "10")]
    read_threads: usize,

    /// Number of write threads
    #[structopt(long, default_value = "90")]
    write_threads: usize,

    /// Payload type
    #[structopt(long, default_value = "devices")]
    payload: String,
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

use crate::db::models::{Device, DeviceValues, User, UserValues};
use anyhow::Result;
use app::App;
use clap::Parser;
use std::sync::Arc;

mod app;
mod db;
mod logging;

#[derive(Debug, Parser, Clone)]
struct Opt {
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

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    dotenv::dotenv().ok();
    logging::init();

    let session = db::connection::builder(true).await?;

    let mut app = App::new();

    let display = app.run(Arc::from(session), &opt).await;

    ratatui::restore();
    display
}

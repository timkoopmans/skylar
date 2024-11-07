use crate::db::models::cache::{Cache, CacheValues};
use crate::db::models::timeseries::{Device, DeviceValues};
use anyhow::Result;
use app::{logging, App};
use clap::Parser;
use std::sync::Arc;

mod app;
mod db;

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
    #[structopt(long, short = 't', default_value = "3000")]
    tablets: usize,

    /// Number of read threads
    #[structopt(long, short = 'R', default_value = "50")]
    readers: usize,

    /// Number of write threads
    #[structopt(long, short = 'W', default_value = "50")]
    writers: usize,

    /// Payload type
    #[structopt(long, short = 'P', default_value = "timeseries")]
    payload: String,

    /// Cardinality
    /// The number of unique values to generate.
    #[structopt(long, short = 'C', default_value = "1000000")]
    cardinality: u64,

    /// Distribution
    /// sequential:
    /// The sequential distribution, where each value is the previous value plus 1.
    /// uniform:
    /// The uniform distribution U(min, max).
    /// normal:
    /// The normal distribution N(mean, std_dev**2).
    /// Note that StandardNormal is an optimised implementation for mean 0, and standard deviation 1.
    /// poisson:
    /// The Poisson distribution Poisson(lambda).
    /// This distribution has a density function: f(k) = lambda^k * exp(-lambda) / k! for k >= 0.
    /// geometric:
    /// The geometric distribution Geometric(p) bounded to [0, u64::MAX].
    /// This is the probability distribution of the number of failures before the first success in a series of Bernoulli trials.
    /// It has the density function f(k) = (1 - p)^k p for k >= 0, where p is the probability of success on each trial.
    /// binomial:
    /// The binomial distribution Binomial(n, p).
    /// This distribution has density function: f(k) = n!/(k! (n-k)!) p^k (1-p)^(n-k) for k >= 0.
    /// zipf:
    /// Samples integers according to the Zipf distribution.
    /// The samples follow Zipf's law: The frequency of each sample from a finite
    /// set of size `n` is inversely proportional to a power of its frequency rank
    /// (with exponent `s`).
    #[structopt(long, short = 'D', default_value = "uniform")]
    distribution: String,

    /// Rate Min
    /// The min rate at which to insert/read data in iterations per second.
    #[structopt(long, default_value = "0")]
    rate_min: u64,

    /// Rate Max
    /// The max rate at which to insert/read data in iterations per second.
    #[structopt(long, default_value = "0")]
    rate_max: u64,

    /// Rate Period
    /// The period over which to increase the rate from rate_min to rate_max.
    #[structopt(long, default_value = "0")]
    rate_period: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    dotenv::dotenv().ok();
    logging::init();

    std::env::set_var("CARDINALITY", opt.cardinality.to_string());

    let session = db::connection::builder(true, &opt).await?;

    let mut app = App::new();

    let result = match opt.payload.as_str() {
        "timeseries" => {
            app.run::<Device, DeviceValues>(Arc::from(session), &opt)
                .await
        }
        "cache" => {
            app.run::<Cache, CacheValues>(Arc::from(session), &opt)
                .await
        }
        _ => panic!("Unsupported payload type"),
    };

    result
}

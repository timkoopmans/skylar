use std::time::Duration;

use crate::db::models::cache::DDL_CACHE;
use crate::db::models::timeseries::DDL_TIMESERIES;
use crate::Opt;
use anyhow::{anyhow, Result};
use scylla::load_balancing::DefaultPolicy;
use scylla::statement::Consistency;
use scylla::transport::ExecutionProfile;
use scylla::{Session, SessionBuilder};
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use tracing::debug;

pub async fn builder(migrate: bool, opt: &Opt) -> Result<Session> {
    let host = opt.host.clone();

    let consistency = match opt.consistency_level.to_uppercase().as_str() {
        "ONE" => Consistency::One,
        "TWO" => Consistency::Two,
        "THREE" => Consistency::Three,
        "QUORUM" => Consistency::Quorum,
        "ALL" => Consistency::All,
        "LOCAL_QUORUM" => Consistency::LocalQuorum,
        "EACH_QUORUM" => Consistency::EachQuorum,
        "SERIAL" => Consistency::Serial,
        "LOCAL_SERIAL" => Consistency::LocalSerial,
        "LOCAL_ONE" => Consistency::LocalOne,
        _ => Consistency::LocalQuorum,
    };

    debug!("Connecting to ScyllaDB at: {}  CL: {}", host, consistency);

    let strategy = ExponentialBackoff::from_millis(500).max_delay(Duration::from_secs(20));

    let session = Retry::spawn(strategy, || async {
        let datacenter = opt.datacenter.clone();

        let default_policy = DefaultPolicy::builder()
            .prefer_datacenter(datacenter)
            .token_aware(true)
            .permit_dc_failover(false)
            .build();

        let profile = ExecutionProfile::builder()
            .load_balancing_policy(default_policy)
            .consistency(consistency)
            .build();

        let handle = profile.into_handle();

        SessionBuilder::new()
            .known_node(&host)
            .default_execution_profile_handle(handle)
            .user(opt.username.clone(), opt.password.clone())
            .build()
            .await
    })
    .await
    .map_err(|e| anyhow!("Error connecting to the database: {}", e))?;

    if migrate {
        let tablets_enabled = if opt.tablets > 0 { "true" } else { "false" };
        let tablets = opt.tablets.to_string();
        let replication_factor = opt.replication_factor.to_string();
        let schema_query = match opt.payload.as_str() {
            "timeseries" => DDL_TIMESERIES,
            "cache" => DDL_CACHE,
            _ => panic!("Unsupported payload type"),
        }
        .trim()
        .replace('\n', " ")
        .replace("<RF>", &replication_factor)
        .replace("<TABLETS>", &tablets)
        .replace("<TABLETS_ENABLED>", tablets_enabled);

        for q in schema_query.split(';') {
            let query = q.to_owned() + ";";
            if !query.starts_with("--") && query.len() > 1 {
                debug!("Running Migration {}", query);
                session
                    .query_unpaged(query, &[])
                    .await
                    .map_err(|e| anyhow!("Error executing migration query: {}", e))?;
            }
        }
    }

    Ok(session)
}

use std::env;
use std::time::Duration;

use crate::db::models::devices::DDL_DEVICES;
use crate::db::models::users::DDL_USERS;
use crate::Opt;
use anyhow::{anyhow, Result};
use scylla::load_balancing::DefaultPolicy;
use scylla::statement::Consistency;
use scylla::transport::ExecutionProfile;
use scylla::{Session, SessionBuilder};
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use tracing::debug;

pub async fn builder(migrate: bool, opt: &Opt) -> Result<Session> {
    let scylladb_url = env::var("SCYLLADB_URL").expect("SCYLLADB_URL must be set");

    let consistency = match env::var("CL").unwrap_or_default().to_uppercase().as_str() {
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

    debug!(
        "Connecting to ScyllaDB at: {}  CL: {}",
        scylladb_url, consistency
    );

    let strategy = ExponentialBackoff::from_millis(500).max_delay(Duration::from_secs(20));

    let session = Retry::spawn(strategy, || async {
        let datacenter = env::var("DATACENTER").unwrap_or("datacenter1".to_string());

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
            .known_node(&scylladb_url)
            .default_execution_profile_handle(handle)
            .build()
            .await
    })
    .await
    .map_err(|e| anyhow!("Error connecting to the database: {}", e))?;

    if migrate {
        let replication_factor = env::var("RF").unwrap_or("1".to_string());
        let schema_query = match opt.payload.as_str() {
            "devices" => DDL_DEVICES,
            "users" => DDL_USERS,
            _ => panic!("Unsupported payload type"),
        }
        .trim()
        .replace('\n', " ")
        .replace("<RF>", &replication_factor);

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

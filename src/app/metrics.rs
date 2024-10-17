use crate::app::App;
use anyhow::{anyhow, Result};
use regex::Regex;
use scylla::Metrics;
use std::collections::HashMap;

impl App {
    pub fn update_metrics(&mut self, metrics: &Metrics) {
        let queries_num_rate = metrics.get_queries_num() - self.queries_num_prev;
        let queries_iter_num_rate = metrics.get_queries_iter_num() - self.queries_iter_num_prev;
        let errors_num_rate = metrics.get_errors_num() - self.errors_num_prev;
        let errors_iter_num_rate = metrics.get_errors_iter_num() - self.errors_iter_num_prev;

        self.queries_num_prev = metrics.get_queries_num();
        self.queries_iter_num_prev = metrics.get_queries_iter_num();
        self.errors_num_prev = metrics.get_errors_num();
        self.errors_iter_num_prev = metrics.get_errors_iter_num();

        self.queries_num.push(queries_num_rate);
        self.queries_iter_num.push(queries_iter_num_rate);
        self.errors_num.push(errors_num_rate);
        self.errors_iter_num.push(errors_iter_num_rate);
        self.latency_avg_ms
            .push(metrics.get_latency_avg_ms().unwrap_or(0));
        self.latency_percentile_ms
            .push(metrics.get_latency_percentile_ms(99.9).unwrap_or(0));

        self.trim_metrics();
    }

    fn trim_metrics(&mut self) {
        if self.queries_num.len() > 100 {
            self.queries_num.remove(0);
        }
        if self.queries_iter_num.len() > 100 {
            self.queries_iter_num.remove(0);
        }
        if self.errors_num.len() > 100 {
            self.errors_num.remove(0);
        }
        if self.errors_iter_num.len() > 100 {
            self.errors_iter_num.remove(0);
        }
        if self.latency_avg_ms.len() > 100 {
            self.latency_avg_ms.remove(0);
        }
        if self.latency_percentile_ms.len() > 100 {
            self.latency_percentile_ms.remove(0);
        }
    }

    #[allow(dead_code)]
    async fn fetch_max_latency_metrics(endpoint: &str) -> Result<HashMap<String, i64>> {
        let client = reqwest::Client::new();
        let response = client.get(endpoint).send().await?.text().await?;

        let re = Regex::new("scylla_storage_proxy_coordinator_(\\w+)_latency_summary\\{quantile=\"0\\.990000\",.*,shard=\"(\\d+)\"\\} (\\d+)")?;

        let mut max_latencies = HashMap::new();

        for line in response.lines() {
            if let Some(caps) = re.captures(line) {
                let operation = caps[1].to_string();
                let latency: i64 = caps[3].parse().unwrap_or(0);

                max_latencies
                    .entry(operation)
                    .and_modify(|e| *e = i64::max(*e, latency))
                    .or_insert(latency);
            }
        }

        if max_latencies.is_empty() {
            return Err(anyhow!("No latency data found"));
        }

        Ok(max_latencies)
    }

    #[allow(dead_code)]
    async fn fetch_total_read_metrics(endpoint: &str) -> Result<i64> {
        let client = reqwest::Client::new();
        let response = client.get(endpoint).send().await?.text().await?;

        let re_total = Regex::new("scylla_cql_reads\\{shard=\"(\\d+)\"\\} (\\d+)")?;
        let re_internal = Regex::new(
            "scylla_cql_reads_per_ks\\{ks=\"system\", shard=\"(\\d+)\", who=\"internal\"\\} (\\d+)",
        )?;

        let mut total_reads = 0;
        let mut internal_reads = 0;

        for line in response.lines() {
            if let Some(caps) = re_total.captures(line) {
                total_reads += caps[2].parse::<i64>().unwrap_or(0);
            } else if let Some(caps) = re_internal.captures(line) {
                internal_reads += caps[2].parse::<i64>().unwrap_or(0);
            }
        }

        let net_reads = total_reads - internal_reads;

        Ok(net_reads)
    }

    #[allow(dead_code)]
    async fn fetch_total_write_metrics(endpoint: &str) -> Result<i64> {
        let client = reqwest::Client::new();
        let response = client.get(endpoint).send().await?.text().await?;

        let re_total =
            Regex::new("scylla_cql_inserts\\{conditional.+?shard=\"(\\d+)\"\\} (\\d+)").unwrap();
        let re_internal = Regex::new("scylla_cql_inserts_per_ks\\{conditional.+?ks=\"system\", shard=\"(\\d+)\", who=\"internal\"\\} (\\d+)")?;

        let mut total_writes = 0;
        let mut internal_writes = 0;

        for line in response.lines() {
            if let Some(caps) = re_total.captures(line) {
                total_writes += caps[2].parse::<i64>().unwrap_or(0);
            } else if let Some(caps) = re_internal.captures(line) {
                internal_writes += caps[2].parse::<i64>().unwrap_or(0);
            }
        }

        let net_reads = total_writes - internal_writes;

        Ok(net_reads)
    }
}

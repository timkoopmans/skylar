mod events;
mod metrics;
mod render;
mod state;
mod system;
mod tabs;
mod tasks;

use crate::app::system::initialize_system;
use crate::db::models::{ReadPayload, WritePayload};
use crate::Opt;
use scylla::Session;
use state::AppState;
use std::sync::Arc;
use sysinfo::System;
use tabs::SelectedTab;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct App {
    queries_num: Vec<u64>,
    queries_iter_num: Vec<u64>,
    errors_num: Vec<u64>,
    errors_iter_num: Vec<u64>,
    latency_avg_ms: Vec<u64>,
    latency_percentile_ms: Vec<u64>,
    queries_num_prev: u64,
    queries_iter_num_prev: u64,
    errors_num_prev: u64,
    errors_iter_num_prev: u64,
    read_logs: Vec<String>,
    cpu_usage: f32,
    memory_usage: f32,
    selected_tab: SelectedTab,
    state: AppState,
    system: Arc<std::sync::Mutex<System>>,
}

impl App {
    pub fn new() -> Self {
        Self {
            queries_num: vec![],
            queries_iter_num: vec![],
            errors_num: vec![],
            errors_iter_num: vec![],
            latency_avg_ms: vec![],
            latency_percentile_ms: vec![],
            queries_num_prev: 0,
            queries_iter_num_prev: 0,
            errors_num_prev: 0,
            errors_iter_num_prev: 0,
            read_logs: vec![],
            cpu_usage: 0.0,
            memory_usage: 0.0,
            selected_tab: SelectedTab::Metrics,
            state: AppState::Running,
            system: initialize_system(),
        }
    }

    pub async fn run<
        W: WritePayload + scylla::serialize::row::SerializeRow + scylla::FromRow + std::fmt::Debug,
        R: ReadPayload + scylla::serialize::row::SerializeRow + scylla::FromRow + std::fmt::Debug,
    >(
        &mut self,
        session: Arc<Session>,
        opt: &Opt,
    ) -> anyhow::Result<()> {
        let (tx, rx) = mpsc::unbounded_channel();
        let cancellation_token = CancellationToken::new();

        let read_task = self.spawn_read_task::<W, R>(
            session.clone(),
            opt.clone(),
            tx.clone(),
            cancellation_token.clone(),
        );

        let write_task =
            self.spawn_write_task::<W>(session.clone(), opt.clone(), cancellation_token.clone());

        let display_task = self.spawn_display_task(session.clone(), cancellation_token.clone(), rx);

        tokio::try_join!(read_task, write_task, display_task)?;

        Ok(())
    }
}

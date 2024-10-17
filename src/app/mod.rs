mod metrics;
mod render;
mod state;
mod system;
mod tabs;

use crate::app::system::initialize_system;
use crate::db::models::{ReadPayload, WritePayload};
use crate::Opt;
use futures::StreamExt;
use ratatui::crossterm::event;
use ratatui::crossterm::event::{Event, KeyCode, KeyEventKind};
use scylla::prepared_statement::PreparedStatement;
use scylla::Session;
use state::AppState;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::System;
use tabs::SelectedTab;
use tokio::sync::{mpsc, Mutex};
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

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

    fn spawn_read_task<W, R>(
        &self,
        session: Arc<Session>,
        opt: Opt,
        tx: mpsc::UnboundedSender<String>,
        cancellation_token: CancellationToken,
    ) -> tokio::task::JoinHandle<()>
    where
        W: WritePayload + scylla::serialize::row::SerializeRow + scylla::FromRow + std::fmt::Debug,
        R: ReadPayload + scylla::serialize::row::SerializeRow + scylla::FromRow + std::fmt::Debug,
    {
        tokio::spawn(async move {
            for _ in 0..opt.readers {
                let session = session.clone();
                let statement: PreparedStatement = session
                    .prepare(R::select_query())
                    .await
                    .expect("Failed to prepare SELECT statement");
                let tx = tx.clone();
                let distribution = opt.distribution.clone();
                let cancellation_token = cancellation_token.clone();
                tokio::spawn(async move {
                    loop {
                        let statement = statement.clone();
                        let payload = R::select_values(distribution.as_str());
                        let mut rows_stream = session
                            .execute_iter(statement, &payload)
                            .await
                            .expect("Failed to execute query")
                            .into_typed::<W>();

                        while let Some(next_row_res) = rows_stream.next().await {
                            match next_row_res {
                                Ok(payload) => {
                                    debug!("{:?}", payload);
                                    if tx.send(format!("{:?}", payload)).is_err() {
                                        debug!("Failed to send row to display task");
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("Error reading payload: {}", e);
                                }
                            }
                        }

                        if cancellation_token.is_cancelled() {
                            break;
                        }
                    }
                });
            }
        })
    }

    fn spawn_write_task<W>(
        &self,
        session: Arc<Session>,
        opt: Opt,
        cancellation_token: CancellationToken,
    ) -> tokio::task::JoinHandle<()>
    where
        W: WritePayload + scylla::serialize::row::SerializeRow + scylla::FromRow + std::fmt::Debug,
    {
        tokio::spawn(async move {
            for _ in 0..opt.writers {
                let session = session.clone();
                let statement: PreparedStatement = session
                    .prepare(W::insert_query())
                    .await
                    .expect("Failed to prepare INSERT statement");
                let distribution = opt.distribution.clone();
                let cancellation_token = cancellation_token.clone();
                tokio::spawn(async move {
                    loop {
                        let payload = W::insert_values(distribution.as_str());
                        if let Err(e) = session.execute_unpaged(&statement, &payload).await {
                            error!("Error inserting payload: {}", e);
                        }

                        if cancellation_token.is_cancelled() {
                            break;
                        }
                    }
                });
            }
        })
    }

    fn spawn_display_task(
        &self,
        session: Arc<Session>,
        cancellation_token: CancellationToken,
        mut rx: mpsc::UnboundedReceiver<String>,
    ) -> tokio::task::JoinHandle<()> {
        let app_data = self.clone();
        let app = Arc::new(Mutex::new(app_data));
        tokio::spawn(async move {
            let mut terminal = ratatui::init();

            loop {
                let metrics = session.get_metrics();
                {
                    let mut app = app.lock().await;
                    app.update_metrics(&metrics);
                    app.update_system();
                }

                while let Ok(row) = rx.try_recv() {
                    let mut app = app.lock().await;
                    app.read_logs.push(row);
                    if app.read_logs.len() > 100 {
                        app.read_logs.remove(0);
                    }
                }

                let mut app = app.lock().await;
                if let Err(e) = terminal.draw(|frame| app.render(frame)) {
                    error!("Error drawing frame: {}", e);
                }

                if let Err(e) = app.handle_events() {
                    error!("Error handling events: {}", e);
                }

                if app.state == AppState::Quitting || cancellation_token.is_cancelled() {
                    debug!("AppState is Quitting or CancellationToken is cancelled, exiting display_task loop");
                    break;
                }

                time::sleep(Duration::from_millis(1000)).await;
            }

            terminal.clear().expect("Failed to clear terminal");
            terminal.show_cursor().expect("Failed to show cursor");
        })
    }

    fn handle_events(&mut self) -> std::io::Result<()> {
        if event::poll(Duration::from_millis(0))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('l') | KeyCode::Right => self.next_tab(),
                        KeyCode::Char('h') | KeyCode::Left => self.previous_tab(),
                        KeyCode::Char('q') | KeyCode::Esc => self.quit(),
                        KeyCode::Char('c')
                            if key.modifiers.contains(event::KeyModifiers::CONTROL) =>
                        {
                            self.quit()
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    pub fn next_tab(&mut self) {
        self.selected_tab = self.selected_tab.next();
    }

    pub fn previous_tab(&mut self) {
        self.selected_tab = self.selected_tab.previous();
    }

    pub fn quit(&mut self) {
        self.state = AppState::Quitting;
    }
}

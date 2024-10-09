use crate::db::models;
use crate::db::models::{generate_random_device, Device, INSERT_DEVICE, SELECT_DEVICE};
use crate::Opt;
use chrono::{Datelike, Utc};
use futures::StreamExt;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::prelude::{Color, Style};
use ratatui::widgets::{Block, Borders, Sparkline};
use ratatui::Frame;
use scylla::prepared_statement::PreparedStatement;
use scylla::{Metrics, Session};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{debug, error};

#[derive(Clone)]
pub struct App {
    queries_num: Vec<u64>,
    queries_iter_num: Vec<u64>,
    errors_num: Vec<u64>,
    errors_iter_num: Vec<u64>,
    latency_avg_ms: Vec<u64>,
    latency_percentile_ms: Vec<u64>,
}

impl App {
    pub(crate) fn new() -> Self {
        Self {
            queries_num: vec![],
            queries_iter_num: vec![],
            errors_num: vec![],
            errors_iter_num: vec![],
            latency_avg_ms: vec![],
            latency_percentile_ms: vec![],
        }
    }

    fn update_metrics(&mut self, metrics: &Metrics) {
        self.queries_num.push(metrics.get_queries_num());
        self.queries_iter_num.push(metrics.get_queries_iter_num());
        self.errors_num.push(metrics.get_errors_num());
        self.errors_iter_num.push(metrics.get_errors_iter_num());
        self.latency_avg_ms
            .push(metrics.get_latency_avg_ms().unwrap_or(0));
        self.latency_percentile_ms
            .push(metrics.get_latency_percentile_ms(99.9).unwrap_or(0));

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

    fn draw(&self, frame: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints(
                [
                    Constraint::Percentage(16),
                    Constraint::Percentage(16),
                    Constraint::Percentage(16),
                    Constraint::Percentage(16),
                    Constraint::Percentage(16),
                    Constraint::Percentage(16),
                ]
                .as_ref(),
            )
            .split(frame.area());

        let latency_avg_ms_title = format!(
            "Average Latency (ms) (Last: {})",
            self.latency_avg_ms.last().unwrap_or(&0)
        );
        let latency_avg_ms_sparkline = Sparkline::default()
            .block(
                Block::default()
                    .title(latency_avg_ms_title)
                    .borders(Borders::ALL),
            )
            .data(&self.latency_avg_ms)
            .style(Style::default().fg(Color::Magenta));
        frame.render_widget(latency_avg_ms_sparkline, chunks[0]);

        let latency_percentile_ms_title = format!(
            "99.9 Latency Percentile (ms) (Last: {})",
            self.latency_percentile_ms.last().unwrap_or(&0)
        );
        let latency_percentile_ms_sparkline = Sparkline::default()
            .block(
                Block::default()
                    .title(latency_percentile_ms_title)
                    .borders(Borders::ALL),
            )
            .data(&self.latency_percentile_ms)
            .style(Style::default().fg(Color::Cyan));
        frame.render_widget(latency_percentile_ms_sparkline, chunks[1]);

        let queries_num_title = format!(
            "Queries Requested (Last: {})",
            self.queries_num.last().unwrap_or(&0)
        );
        let queries_num_sparkline = Sparkline::default()
            .block(
                Block::default()
                    .title(queries_num_title)
                    .borders(Borders::ALL),
            )
            .data(&self.queries_num)
            .style(Style::default().fg(Color::LightBlue));
        frame.render_widget(queries_num_sparkline, chunks[2]);

        let queries_iter_num_title = format!(
            "Iter Queries Requested (Last: {})",
            self.queries_iter_num.last().unwrap_or(&0)
        );
        let queries_iter_num_sparkline = Sparkline::default()
            .block(
                Block::default()
                    .title(queries_iter_num_title)
                    .borders(Borders::ALL),
            )
            .data(&self.queries_iter_num)
            .style(Style::default().fg(Color::Green));
        frame.render_widget(queries_iter_num_sparkline, chunks[3]);

        let errors_num_title = format!(
            "Errors Occurred (Last: {})",
            self.errors_num.last().unwrap_or(&0)
        );
        let errors_num_sparkline = Sparkline::default()
            .block(
                Block::default()
                    .title(errors_num_title)
                    .borders(Borders::ALL),
            )
            .data(&self.errors_num)
            .style(Style::default().fg(Color::Red));
        frame.render_widget(errors_num_sparkline, chunks[4]);

        let errors_iter_num_title = format!(
            "Iter Errors Occurred (Last: {})",
            self.errors_iter_num.last().unwrap_or(&0)
        );
        let errors_iter_num_sparkline = Sparkline::default()
            .block(
                Block::default()
                    .title(errors_iter_num_title)
                    .borders(Borders::ALL),
            )
            .data(&self.errors_iter_num)
            .style(Style::default().fg(Color::Yellow));
        frame.render_widget(errors_iter_num_sparkline, chunks[5]);
    }

    pub(crate) async fn run(&mut self, session: Arc<Session>, opt: &Opt) -> anyhow::Result<()> {
        let opt = opt.clone();
        let session_clone = session.clone();

        let read_task = tokio::spawn(async move {
            for _ in 0..opt.read_threads {
                let session = session_clone.clone();
                let statement: PreparedStatement = session
                    .prepare(SELECT_DEVICE)
                    .await
                    .expect("Failed to prepare statement");
                let now = Utc::now();
                let year = now.year();
                let month = now.month() as i32;
                tokio::spawn(async move {
                    loop {
                        let statement = statement.clone();
                        let mut interval = time::interval(Duration::from_millis(10));
                        let rack_id = models::random_rack_id();
                        let sled_id = models::random_sled_id();
                        let mut rows_stream = session
                            .execute_iter(statement, (year, month, rack_id, sled_id))
                            .await
                            .expect("Failed to execute query")
                            .into_typed::<Device>();

                        while let Some(next_row_res) = rows_stream.next().await {
                            match next_row_res {
                                Ok(device) => {
                                    debug!("Device: {:?}", device);
                                }
                                Err(e) => {
                                    error!("Error reading device: {}", e);
                                }
                            }
                        }

                        interval.tick().await;
                    }
                });
            }
        });

        let opt = opt.clone();
        let session_clone = session.clone();
        let write_task = tokio::spawn(async move {
            for _ in 0..opt.write_threads {
                let session = session_clone.clone();
                let statement: PreparedStatement = session
                    .prepare(INSERT_DEVICE)
                    .await
                    .expect("Failed to prepare statement");
                tokio::spawn(async move {
                    loop {
                        let mut interval = time::interval(Duration::from_millis(10));
                        let device = generate_random_device();
                        if let Err(e) = session.execute_unpaged(&statement, &device).await {
                            error!("Error inserting device: {}", e);
                        }
                        interval.tick().await;
                    }
                });
            }
        });

        let app_data = self.clone();
        let app = Arc::new(Mutex::new(app_data));
        let session_clone = session.clone();

        let display_task = tokio::spawn(async move {
            let mut terminal = ratatui::init();

            loop {
                let metrics = session_clone.get_metrics();
                {
                    let mut app = app.lock().await;
                    app.update_metrics(&metrics);
                }

                let app = app.lock().await;
                if let Err(e) = terminal.draw(|frame| app.draw(frame)) {
                    error!("Error drawing frame: {}", e);
                }

                time::sleep(Duration::from_secs(1)).await;
            }
        });

        tokio::try_join!(read_task, write_task, display_task)?;

        loop {
            time::sleep(Duration::from_secs(60)).await;
        }
    }
}

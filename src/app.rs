use crate::db::models::{ReadPayload, WritePayload};
use crate::Opt;
use futures::StreamExt;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::prelude::{Color, Style};
use ratatui::widgets::{Block, Borders, Sparkline};
use ratatui::widgets::{List, ListItem};
use ratatui::Frame;
use scylla::prepared_statement::PreparedStatement;
use scylla::{Metrics, Session};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
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
    queries_num_prev: u64,
    queries_iter_num_prev: u64,
    errors_num_prev: u64,
    errors_iter_num_prev: u64,
    read_logs: Vec<String>,
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
            queries_num_prev: 0,
            queries_iter_num_prev: 0,
            errors_num_prev: 0,
            errors_iter_num_prev: 0,
            read_logs: vec![],
        }
    }

    fn update_metrics(&mut self, metrics: &Metrics) {
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
                    Constraint::Percentage(14),
                    Constraint::Percentage(14),
                    Constraint::Percentage(14),
                    Constraint::Percentage(14),
                    Constraint::Percentage(10),
                    Constraint::Percentage(10),
                    Constraint::Percentage(24),
                ]
                .as_ref(),
            )
            .split(frame.area());

        let latency_avg_ms_title = format!(
            "Average Latency ({}ms)",
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
            "99.9 Latency Percentile ({}ms)",
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
            "Queries Requested ({}/s)",
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
            "Iter Queries Requested ({}/s)",
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
            "Errors Occurred ({}/s)",
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
            "Iter Errors Occurred ({}/s)",
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

        let items: Vec<ListItem> = self
            .read_logs
            .iter()
            .map(|i| ListItem::new(i.as_str()))
            .collect();
        let read_logs_list = List::new(items)
            .block(Block::default().title("Read Log").borders(Borders::ALL))
            .style(Style::default().fg(Color::White));
        frame.render_widget(read_logs_list, chunks[6]);
    }

    pub async fn run<
        W: WritePayload + scylla::serialize::row::SerializeRow + scylla::FromRow + std::fmt::Debug,
        R: ReadPayload + scylla::serialize::row::SerializeRow + scylla::FromRow + std::fmt::Debug,
    >(
        &mut self,
        session: Arc<Session>,
        opt: &Opt,
    ) -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(100);

        let session_clone = session.clone();
        let opt = opt.clone();
        let read_task = tokio::spawn(async move {
            for _ in 0..opt.read_threads {
                let session = session_clone.clone();
                let statement: PreparedStatement = session
                    .prepare(R::select_query())
                    .await
                    .expect("Failed to prepare SELECT statement");
                let tx = tx.clone();
                tokio::spawn(async move {
                    loop {
                        let statement = statement.clone();
                        let payload = R::select_values();
                        let mut rows_stream = session
                            .execute_iter(statement, &payload)
                            .await
                            .expect("Failed to execute query")
                            .into_typed::<W>();

                        while let Some(next_row_res) = rows_stream.next().await {
                            match next_row_res {
                                Ok(payload) => {
                                    debug!("{:?}", payload);
                                    if tx.send(format!("{:?}", payload)).await.is_err() {
                                        error!("Failed to send row to display task");
                                    }
                                }
                                Err(e) => {
                                    error!("Error reading payload: {}", e);
                                }
                            }
                        }
                    }
                });
            }
        });

        let session_clone = session.clone();
        let opt = opt.clone();
        let write_task = tokio::spawn(async move {
            for _ in 0..opt.write_threads {
                let session = session_clone.clone();
                let statement: PreparedStatement = session
                    .prepare(W::insert_query())
                    .await
                    .expect("Failed to prepare INSERT statement");
                tokio::spawn(async move {
                    loop {
                        let payload = W::insert_values();
                        if let Err(e) = session.execute_unpaged(&statement, &payload).await {
                            error!("Error inserting payload: {}", e);
                        }
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

                while let Ok(row) = rx.try_recv() {
                    let mut app = app.lock().await;
                    app.read_logs.push(row);
                    if app.read_logs.len() > 100 {
                        app.read_logs.remove(0);
                    }
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

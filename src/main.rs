use crate::models::{generate_random_device, Device, INSERT_DEVICE, SELECT_DEVICE};
use anyhow::Result;
use chrono::{Datelike, Utc};
use clap::Parser;
use futures::stream::StreamExt;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style, Stylize};
use ratatui::widgets::Sparkline;

use ratatui::text::Line;
use ratatui::widgets::{Bar, BarChart, BarGroup, Block, Borders};
use ratatui::Frame;
use scylla::prepared_statement::PreparedStatement;
use scylla::Session;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{self, Duration};
use tracing::{debug, error, info};

pub mod db;
mod logging;
mod models;

#[derive(Debug, Parser, Clone)]
struct Opt {
    /// Number of read threads
    #[structopt(long, default_value = "10")]
    read_threads: usize,

    /// Number of write threads
    #[structopt(long, default_value = "10")]
    write_threads: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    dotenv::dotenv().ok();
    logging::init();

    let session = db::connection::builder(true).await?;

    let mut app = App::new(vec![]);

    let app_result = app.run(Arc::from(session), &opt).await;
    ratatui::restore();
    app_result
}

#[derive(Clone)]
struct App {
    bytes_sent: Vec<i32>,
    bytes_sent_sparkline: Vec<u64>,
    bytes_received: Vec<i32>,
    bytes_received_sparkline: Vec<u64>,
}

impl App {
    fn new(devices: Vec<Device>) -> Self {
        let bytes_sent: Vec<i32> = devices.iter().map(|d| d.bytes_sent).collect();
        let bytes_sent_sparkline: Vec<u64> = bytes_sent.iter().map(|&b| b as u64).collect();
        let bytes_received: Vec<i32> = devices.iter().map(|d| d.bytes_received).collect();
        let bytes_received_sparkline: Vec<u64> = bytes_received.iter().map(|&b| b as u64).collect();

        Self {
            bytes_sent,
            bytes_sent_sparkline,
            bytes_received,
            bytes_received_sparkline,
        }
    }

    fn update_bytes_sent(&mut self, devices: &Vec<Device>) {
        for device in devices {
            self.bytes_sent.push(device.bytes_sent);
            self.bytes_sent_sparkline.push(device.bytes_sent as u64);
            self.bytes_received.push(device.bytes_received);
            self.bytes_received_sparkline
                .push(device.bytes_received as u64);

            if self.bytes_sent.len() > 100 {
                self.bytes_sent.remove(0);
            }
            if self.bytes_sent_sparkline.len() > 100 {
                self.bytes_sent_sparkline.remove(0);
            }
            if self.bytes_received.len() > 100 {
                self.bytes_received.remove(0);
            }
            if self.bytes_received_sparkline.len() > 100 {
                self.bytes_received_sparkline.remove(0);
            }
        }
    }

    fn draw(&self, frame: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints(
                [
                    Constraint::Percentage(10),
                    Constraint::Percentage(40),
                    Constraint::Percentage(40),
                    Constraint::Percentage(10),
                ]
                .as_ref(),
            )
            .split(frame.area());

        let bytes_sent_sparkline = Sparkline::default()
            .block(Block::default().title("Bytes Sent").borders(Borders::ALL))
            .data(&self.bytes_sent_sparkline)
            .style(Style::default().fg(Color::LightBlue));
        frame.render_widget(bytes_sent_sparkline, chunks[1]);

        let bytes_received_sparkline = Sparkline::default()
            .block(
                Block::default()
                    .title("Bytes Received")
                    .borders(Borders::ALL),
            )
            .data(&self.bytes_received_sparkline)
            .style(Style::default().fg(Color::Green));
        frame.render_widget(bytes_received_sparkline, chunks[2]);
    }

    async fn run(&mut self, session: Arc<Session>, opt: &Opt) -> Result<()> {
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

        let app_data = self.clone(); // Clone the necessary data
        let app = Arc::new(Mutex::new(app_data));
        let session_clone = session.clone();

        let display_task = tokio::spawn(async move {
            let mut terminal = ratatui::init();
            let statement: PreparedStatement = session_clone
                .prepare(SELECT_DEVICE)
                .await
                .expect("Failed to prepare statement");
            let now = Utc::now();
            let year = now.year();
            let month = now.month() as i32;

            loop {
                let statement = statement.clone();
                let mut interval = time::interval(Duration::from_millis(1000));
                let rack_id = models::random_rack_id();
                let sled_id = models::random_sled_id();
                let mut rows_stream = session_clone
                    .execute_iter(statement, (year, month, rack_id, sled_id))
                    .await
                    .expect("Failed to execute query")
                    .into_typed::<Device>();

                while let Some(next_row_res) = rows_stream.next().await {
                    match next_row_res {
                        Ok(device) => {
                            debug!("Device: {:?}", device);
                            let mut app = app.lock().await;
                            app.update_bytes_sent(&vec![device]);
                        }
                        Err(e) => {
                            error!("Error reading device: {}", e);
                        }
                    }
                }

                let app = app.lock().await;
                if let Err(e) = terminal.draw(|frame| app.draw(frame)) {
                    error!("Error drawing frame: {}", e);
                }

                interval.tick().await;
            }
        });

        tokio::try_join!(read_task, write_task, display_task)?;

        loop {
            time::sleep(Duration::from_secs(60)).await;
        }
    }
}

use crate::models::{generate_random_device, Device, INSERT_DEVICE, SELECT_DEVICE};
use anyhow::Result;
use chrono::{Datelike, Utc};
use clap::Parser;
use futures::stream::StreamExt;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style, Stylize};
use ratatui::text::Line;
use ratatui::widgets::{Bar, BarChart, BarGroup, Block, Borders};
use ratatui::Frame;
use scylla::prepared_statement::PreparedStatement;
use scylla::Session;
use std::sync::Arc;
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

struct App {
    bytes_sent: Vec<i32>,
}

impl App {
    fn new(devices: Vec<Device>) -> Self {
        let bytes_sent: Vec<i32> = devices.iter().map(|d| d.bytes_sent).collect();

        Self { bytes_sent }
    }

    fn update_bytes_sent(&mut self, devices: &Vec<Device>) {
        for device in devices {
            self.bytes_sent.push(device.bytes_sent);
            if self.bytes_sent.len() > 20 {
                self.bytes_sent.remove(0);
            }
        }
    }

    async fn run(&mut self, session: Arc<Session>, opt: &Opt) -> Result<()> {
        // let mut terminal = ratatui::init();
        //
        // let mut interval = time::interval(Duration::from_millis(10));
        // self.update_bytes_sent(&vec![generate_random_device()]);
        // if let Err(e) = terminal.draw(|frame| self.draw(frame)) {
        //     error!("Error drawing frame: {}", e);
        // }
        // interval.tick().await;

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

        tokio::try_join!(read_task, write_task)?;

        loop {
            time::sleep(Duration::from_secs(60)).await;
        }
    }

    fn draw(&self, frame: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints(
                [
                    Constraint::Percentage(10),
                    Constraint::Percentage(80),
                    Constraint::Percentage(10),
                ]
                .as_ref(),
            )
            .split(frame.area());

        frame.render_widget(
            Block::default().title("Barchart").borders(Borders::ALL),
            chunks[0],
        );
        frame.render_widget(vertical_barchart(&self.bytes_sent), chunks[1]);
    }
}

fn vertical_barchart(bytes_sent: &[i32]) -> BarChart {
    let bars: Vec<Bar> = bytes_sent
        .iter()
        .enumerate()
        .map(|(hour, value)| vertical_bar(hour, value))
        .collect();
    let title = Line::from("Device Temperatures (Vertical)").centered();
    BarChart::default()
        .data(BarGroup::default().bars(&bars))
        .block(Block::default().title(title).borders(Borders::ALL))
        .bar_width(5)
}

fn vertical_bar(hour: usize, bytes_sent: &i32) -> Bar {
    Bar::default()
        .value(*bytes_sent as u64)
        .label(Line::from(format!("{hour:>02}:00")))
        .text_value(format!("{bytes_sent:>3}°"))
        .style(bytes_sent_style(*bytes_sent))
        .value_style(bytes_sent_style(*bytes_sent).reversed())
}

fn bytes_sent_style(value: i32) -> Style {
    let green = (255.0 * (1.0 - f64::from(value - 50) / 40.0)) as u8;
    let color = Color::Rgb(255, green, 0);
    Style::default().fg(color)
}

use crate::models::{generate_random_device, Device, INSERT_DEVICE};
use anyhow::Result;
use clap::Parser;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style, Stylize};
use ratatui::text::Line;
use ratatui::widgets::{Bar, BarChart, BarGroup, Block, Borders};
use ratatui::Frame;
use scylla::prepared_statement::PreparedStatement;
use tokio::time::{self, Duration};
use tracing::{error, info};

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

    db::connection::builder(true).await?;

    let mut app = App::new(vec![]);

    let app_result = app.run(&opt).await;
    ratatui::restore();
    app_result
}

struct App {
    temperatures: Vec<i32>,
}

impl App {
    fn new(devices: Vec<Device>) -> Self {
        let temperatures: Vec<i32> = devices.iter().map(|d| d.temperature).collect();

        Self { temperatures }
    }

    fn update_temperatures(&mut self, devices: &Vec<Device>) {
        for device in devices {
            self.temperatures.push(device.temperature);
            if self.temperatures.len() > 20 {
                self.temperatures.remove(0);
            }
        }
    }

    async fn run(&mut self, opt: &Opt) -> Result<()> {
        let mut terminal = ratatui::init();

        let mut interval = time::interval(Duration::from_millis(10));
        self.update_temperatures(&vec![generate_random_device()]);
        if let Err(e) = terminal.draw(|frame| self.draw(frame)) {
            error!("Error drawing frame: {}", e);
        }
        interval.tick().await;

        let opt = opt.clone();
        let read_task = tokio::spawn(async move {
            for _ in 0..opt.read_threads {
                tokio::spawn(async move {
                    loop {
                        // Simulate read operation
                        // Replace with actual read logic
                        time::sleep(Duration::from_secs(1)).await;
                    }
                });
            }
        });

        let opt = opt.clone();
        let write_task = tokio::spawn(async move {
            for _ in 0..opt.write_threads {
                let session = db::connection::builder(false)
                    .await
                    .expect("Failed to create session");
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
                        info!("Inserted device: {:?}", device);
                        interval.tick().await;
                    }
                });
            }
        });

        tokio::try_join!(read_task, write_task)?;

        Ok(())
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
        frame.render_widget(vertical_barchart(&self.temperatures), chunks[1]);
    }
}

fn vertical_barchart(temperatures: &[i32]) -> BarChart {
    let bars: Vec<Bar> = temperatures
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

fn vertical_bar(hour: usize, temperature: &i32) -> Bar {
    Bar::default()
        .value(*temperature as u64)
        .label(Line::from(format!("{hour:>02}:00")))
        .text_value(format!("{temperature:>3}°"))
        .style(temperature_style(*temperature))
        .value_style(temperature_style(*temperature).reversed())
}

fn temperature_style(value: i32) -> Style {
    let green = (255.0 * (1.0 - f64::from(value - 50) / 40.0)) as u8;
    let color = Color::Rgb(255, green, 0);
    Style::default().fg(color)
}

use crate::db::models::{ReadPayload, WritePayload};
use crate::Opt;
use futures::StreamExt;
use ratatui::crossterm::event;
use ratatui::crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Gauge, List, ListItem, Sparkline, Tabs};
use ratatui::Frame;
use scylla::prepared_statement::PreparedStatement;
use scylla::{Metrics, Session};
use std::sync::Arc;
use std::time::Duration;
use strum::{Display, EnumIter, FromRepr, IntoEnumIterator};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
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

#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum AppState {
    #[default]
    Running,
    Quitting,
}

#[derive(Default, Clone, Copy, Display, FromRepr, EnumIter)]
enum SelectedTab {
    #[default]
    #[strum(to_string = "METRICS")]
    Metrics,
    #[strum(to_string = "SAMPLES")]
    Samples,
    #[strum(to_string = "SYSTEM")]
    System,
}

impl App {
    pub(crate) fn new() -> Self {
        let system = Arc::new(std::sync::Mutex::new(System::new_with_specifics(
            RefreshKind::new()
                .with_cpu(CpuRefreshKind::new())
                .with_memory(MemoryRefreshKind::new()),
        )));
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
            system,
        }
    }

    fn update_system_info(&mut self) {
        let mut system = self.system.lock().unwrap();
        system.refresh_cpu_all();
        system.refresh_memory();
        self.cpu_usage = system.global_cpu_usage();
        self.memory_usage = system.used_memory() as f32 / system.total_memory() as f32 * 100.0;
    }

    fn render_system(&self, frame: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
            .split(area);

        let cpu_gauge = Gauge::default()
            .block(Block::default().title("CPU").borders(Borders::ALL))
            .gauge_style(Style::default().fg(Color::Blue))
            .percent(self.cpu_usage as u16);
        frame.render_widget(cpu_gauge, chunks[0]);

        let memory_gauge = Gauge::default()
            .block(Block::default().title("MEM").borders(Borders::ALL))
            .gauge_style(Style::default().fg(Color::LightBlue))
            .percent(self.memory_usage as u16);
        frame.render_widget(memory_gauge, chunks[1]);
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
            .constraints([Constraint::Length(3), Constraint::Min(0)].as_ref())
            .split(frame.area());

        self.render_tabs(chunks[0], frame);
        match self.selected_tab {
            SelectedTab::Metrics => self.draw_metrics(frame, chunks[1]),
            SelectedTab::Samples => self.draw_read_samples(frame, chunks[1]),
            SelectedTab::System => self.render_system(frame, chunks[1]),
        }
    }

    fn render_tabs(&self, area: Rect, frame: &mut Frame) {
        let titles = SelectedTab::iter()
            .map(|tab| tab.to_string())
            .collect::<Vec<_>>();
        let tabs = Tabs::new(titles)
            .select(self.selected_tab as usize)
            .block(Block::default().borders(Borders::NONE))
            .highlight_style(Style::default().fg(Color::LightBlue));
        frame.render_widget(tabs, area);
    }

    fn draw_metrics(&self, frame: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(10),
                    Constraint::Percentage(10),
                ]
                .as_ref(),
            )
            .split(area);

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
            .style(Style::default().fg(Color::Blue));
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
            .style(Style::default().fg(Color::LightBlue));
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
            .style(Style::default().fg(Color::Green));
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
            .style(Style::default().fg(Color::LightGreen));
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
            .style(Style::default().fg(Color::LightRed));
        frame.render_widget(errors_iter_num_sparkline, chunks[5]);
    }

    fn draw_read_samples(&self, frame: &mut Frame, area: Rect) {
        let items: Vec<ListItem> = self
            .read_logs
            .iter()
            .map(|i| ListItem::new(i.as_str()))
            .collect();
        let read_logs_list = List::new(items)
            .block(Block::default().title("Read Samples").borders(Borders::ALL))
            .style(Style::default().fg(Color::White));
        frame.render_widget(read_logs_list, area);
    }

    pub async fn run<
        W: WritePayload + scylla::serialize::row::SerializeRow + scylla::FromRow + std::fmt::Debug,
        R: ReadPayload + scylla::serialize::row::SerializeRow + scylla::FromRow + std::fmt::Debug,
    >(
        &mut self,
        session: Arc<Session>,
        opt: &Opt,
    ) -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let cancellation_token = CancellationToken::new();

        let session_clone = session.clone();
        let opt_clone = opt.clone();

        let read_task = {
            let cancellation_token = cancellation_token.clone();
            tokio::spawn(async move {
                for _ in 0..opt_clone.readers {
                    let session = session_clone.clone();
                    let statement: PreparedStatement = session
                        .prepare(R::select_query())
                        .await
                        .expect("Failed to prepare SELECT statement");
                    let tx = tx.clone();
                    let distribution = opt_clone.distribution.clone();
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
        };

        let session_clone = session.clone();
        let opt_clone = opt.clone();
        let write_task = {
            let cancellation_token = cancellation_token.clone();
            tokio::spawn(async move {
                for _ in 0..opt_clone.writers {
                    let session = session_clone.clone();
                    let statement: PreparedStatement = session
                        .prepare(W::insert_query())
                        .await
                        .expect("Failed to prepare INSERT statement");
                    let distribution = opt_clone.distribution.clone();
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
        };

        let app_data = self.clone();
        let app = Arc::new(Mutex::new(app_data));
        let session_clone = session.clone();
        let display_task = {
            let cancellation_token = cancellation_token.clone();
            tokio::spawn(async move {
                let mut terminal = ratatui::init();

                loop {
                    let metrics = session_clone.get_metrics();
                    {
                        let mut app = app.lock().await;
                        app.update_metrics(&metrics);
                        app.update_system_info();
                    }

                    while let Ok(row) = rx.try_recv() {
                        let mut app = app.lock().await;
                        app.read_logs.push(row);
                        if app.read_logs.len() > 100 {
                            app.read_logs.remove(0);
                        }
                    }

                    let mut app = app.lock().await;
                    if let Err(e) = terminal.draw(|frame| app.draw(frame)) {
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
        };

        tokio::try_join!(read_task, write_task, display_task)?;

        Ok(())
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

impl SelectedTab {
    fn previous(self) -> Self {
        let current_index: usize = self as usize;
        let previous_index = current_index.saturating_sub(1);
        Self::from_repr(previous_index).unwrap_or(self)
    }

    fn next(self) -> Self {
        let current_index = self as usize;
        let next_index = current_index.saturating_add(1);
        Self::from_repr(next_index).unwrap_or(self)
    }
}

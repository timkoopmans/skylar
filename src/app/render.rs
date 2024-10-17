use crate::app::tabs::SelectedTab;
use crate::app::App;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Gauge, List, ListItem, Sparkline, Tabs};
use ratatui::Frame;
use strum::IntoEnumIterator;

impl App {
    pub fn render(&self, frame: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([Constraint::Length(3), Constraint::Min(0)].as_ref())
            .split(frame.area());

        self.render_tabs(chunks[0], frame);
        match self.selected_tab {
            SelectedTab::Metrics => self.render_metrics(frame, chunks[1]),
            SelectedTab::Samples => self.render_samples(frame, chunks[1]),
            SelectedTab::System => self.render_system(frame, chunks[1]),
        }
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

    fn render_metrics(&self, frame: &mut Frame, area: Rect) {
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

        self.render_sparkline(
            frame,
            chunks[0],
            "Average Latency",
            "ms",
            &self.latency_avg_ms,
            Color::Blue,
        );
        self.render_sparkline(
            frame,
            chunks[1],
            "P99.9 Latency",
            "ms",
            &self.latency_percentile_ms,
            Color::LightBlue,
        );
        self.render_sparkline(
            frame,
            chunks[2],
            "Writes",
            "ops/s",
            &self.queries_num,
            Color::Green,
        );
        self.render_sparkline(
            frame,
            chunks[3],
            "Reads",
            "ops/s",
            &self.queries_iter_num,
            Color::LightGreen,
        );
        self.render_sparkline(
            frame,
            chunks[4],
            "Write Errors",
            "ops/s",
            &self.errors_num,
            Color::Red,
        );
        self.render_sparkline(
            frame,
            chunks[5],
            "Read Errors",
            "ops/s",
            &self.errors_iter_num,
            Color::LightRed,
        );
    }

    fn render_sparkline(
        &self,
        frame: &mut Frame,
        area: Rect,
        title: &str,
        unit: &str,
        data: &[u64],
        color: Color,
    ) {
        let title = format!("{} ({} {})", title, data.last().unwrap_or(&0), unit);
        let sparkline = Sparkline::default()
            .block(Block::default().title(title).borders(Borders::ALL))
            .data(data)
            .style(Style::default().fg(color));
        frame.render_widget(sparkline, area);
    }

    fn render_samples(&self, frame: &mut Frame, area: Rect) {
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
}

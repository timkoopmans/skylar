use crate::app::state::AppState;
use crate::app::App;
use ratatui::crossterm::event;
use ratatui::crossterm::event::{Event, KeyCode, KeyEventKind};
use std::time::Duration;

impl App {
    pub fn handle_events(&mut self) -> std::io::Result<()> {
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

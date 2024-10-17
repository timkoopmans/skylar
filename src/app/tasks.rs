use crate::app::state::AppState;
use crate::app::App;
use crate::db::models::{ReadPayload, WritePayload};
use crate::Opt;
use futures::StreamExt;
use scylla::prepared_statement::PreparedStatement;
use scylla::Session;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

impl App {
    pub fn spawn_read_task<W, R>(
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

    pub fn spawn_write_task<W>(
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

    pub fn spawn_display_task(
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
}
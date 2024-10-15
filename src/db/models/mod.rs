pub mod devices;
pub mod users;

pub trait WritePayload: Send + Sync + 'static {
    fn insert_query() -> &'static str;
    fn insert_values(distribution: &str) -> Self;
}

pub trait ReadPayload: Send + Sync + 'static {
    fn select_query() -> &'static str;
    fn select_values(distribution: &str) -> Self;
}

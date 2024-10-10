use crate::db::models::{ReadPayload, WritePayload};
use chrono::{DateTime, Utc};
use rand::distributions::{Alphanumeric, DistString};
use scylla::{FromRow, SerializeRow};
use uuid::Uuid;

pub const DDL_USERS: &str = r#"
    CREATE KEYSPACE IF NOT EXISTS skylar WITH replication =
    {'class': 'NetworkTopologyStrategy', 'replication_factor': <RF>};

    USE skylar;
    CREATE TABLE IF NOT EXISTS skylar.users
    (
        user_id    uuid PRIMARY KEY,
        username   text,
        email      text,
        created_at timestamp
    )
"#;

pub const INSERT_USER: &str = "
    INSERT INTO skylar.users
    (
        user_id,
        username,
        email,
        created_at
    )
    VALUES (?, ?, ?, ?)
";

pub const SELECT_USER: &str = "
    SELECT
        user_id,
        username,
        email,
        created_at
    FROM skylar.users
    WHERE user_id = ?
";

#[derive(Debug, Clone, SerializeRow, FromRow)]
pub struct User {
    pub user_id: Uuid,
    pub username: String,
    pub email: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, SerializeRow, FromRow)]
pub struct UserValues {
    user_id: Uuid,
}

impl WritePayload for User {
    fn insert_query() -> &'static str {
        INSERT_USER
    }

    fn insert_values() -> Self {
        let mut rng = rand::thread_rng();
        User {
            user_id: Uuid::new_v4(),
            username: Alphanumeric.sample_string(&mut rng, 8),
            email: format!("{}@example.com", Alphanumeric.sample_string(&mut rng, 8)),
            created_at: Utc::now(),
        }
    }
}

impl ReadPayload for UserValues {
    fn select_query() -> &'static str {
        SELECT_USER
    }

    fn select_values() -> Self {
        UserValues {
            user_id: Uuid::new_v4(),
        }
    }
}

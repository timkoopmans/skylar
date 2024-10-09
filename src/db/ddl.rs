pub const DDL_DEVICES: &str = r#"
    CREATE KEYSPACE IF NOT EXISTS skylar WITH replication =
    {'class': 'NetworkTopologyStrategy', 'replication_factor': <RF>};

    USE skylar;
    CREATE TABLE IF NOT EXISTS skylar.devices
    (
        kind             text,
        link_name        text,
        rack_id          uuid,
        sled_id          uuid,
        sled_model       text,
        sled_revision    int,
        sled_serial      text,
        zone_name        text,
        bytes_sent       int,
        bytes_received   int,
        packets_sent     int,
        packets_received int,
        time             timestamp,
        year             int,
        month            int,
        PRIMARY KEY ((year, month, rack_id, sled_id), time)
    )
"#;

// TODO: not implemented
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

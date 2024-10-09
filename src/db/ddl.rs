pub const DDL: &str = r#"
    CREATE KEYSPACE IF NOT EXISTS skylar WITH replication = 
    {'class': 'NetworkTopologyStrategy', 'replication_factor': <RF>};

    USE skylar;
    CREATE TABLE IF NOT EXISTS skylar.devices
    (
        device                      text,
        temperature                 int,
        time                        timestamp,
        year                        int,
        month                       int,
        PRIMARY KEY ((year, month, device), time)
    )
    "#;

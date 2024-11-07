# skylar

![img.png](img.png)
A simple load testing tool for ScyllaDB

```bash
Usage: skylar [OPTIONS]

Options:
      --host <HOST>
          Host [default: localhost:9042]
      --username <USERNAME>
          Username [default: cassandra]
      --password <PASSWORD>
          Password [default: cassandra]
  -c, --consistency-level <CONSISTENCY_LEVEL>
          Consistency level [default: LOCAL_QUORUM]
  -r, --replication-factor <REPLICATION_FACTOR>
          Replication factor [default: 3]
  -d, --datacenter <DATACENTER>
          Datacenter [default: datacenter1]
  -t, --tablets <TABLETS>
          Number of tablets, if set to 0 tablets are disabled [default: 0]
  -R, --readers <READERS>
          Number of read threads [default: 10]
  -W, --writers <WRITERS>
          Number of write threads [default: 90]
  -P, --payload <PAYLOAD>
          Payload type [default: devices]
  -D, --distribution <DISTRIBUTION>
          Distribution sequential, uniform, normal, poisson, geometric, binomial, zipf [default: uniform]      
      --rate-min <RATE_MIN>
          Rate Min The min rate at which to insert/read data in iterations per second [default: 0]
      --rate-max <RATE_MAX>
          Rate Max The max rate at which to insert/read data in iterations per second [default: 0]
      --rate-period <RATE_PERIOD>
          Rate Period The period over which to increase the rate from rate_min to rate_max [default: 0]
    
  -h, --help
          Print help
```

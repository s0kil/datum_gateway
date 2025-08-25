# OCEAN DATUM Gateway - QuestDB Integration

Real-time hashrate monitoring and analytics for ASIC miners using QuestDB time-series database.

## Quick Start

### 1. Install QuestDB

```bash
# Docker (recommended)
docker run -d -p 9000:9000 -p 9009:9009 \
  -v questdb_data:/var/lib/questdb \
  --name questdb questdb/questdb:latest

# Access web console: http://localhost:9000
# Port 9000: HTTP API (web console)
# Port 9009: TCP ILP (used by DATUM Gateway)
```

### 2. Configure DATUM Gateway

Add to your `datum_gateway_config.json`:

```json
{
  "questdb": {
    "enabled": true,
    "host": "localhost",
    "ilp_port": 9009,
    "batch_size": 100,
    "flush_interval_ms": 5000
  },
  "metrics_collector": {
    "enabled": true,
    "collection_interval_sec": 30,
    "hashrate_window_sec": 300
  }
}
```

### 3. Build and Run

```bash
cmake . && make
./datum_gateway -c datum_gateway_config.json
```

## Data Schema

### `ocean_datum_hashrate` - Periodic Metrics (every 30s)

| Column              | Type      | Description                            |
| ------------------- | --------- | -------------------------------------- |
| timestamp           | timestamp | Measurement time                       |
| worker              | symbol    | Worker/miner ID                        |
| ip                  | symbol    | Machine IP address                     |
| user_agent          | symbol    | Mining software (e.g., "xminer-1.2.6") |
| calculated_th       | double    | Pool-calculated hashrate (TH/s)        |
| shares_accepted     | long      | Total accepted shares                  |
| shares_rejected     | long      | Total rejected shares                  |
| diff_accepted       | long      | Total accepted difficulty              |
| diff_rejected       | long      | Total rejected difficulty              |
| shares_per_min      | double    | Shares submitted per minute            |
| uptime_seconds      | long      | Worker uptime since first share        |
| last_share_delay_ms | long      | Time since last share (ms)             |
| current_diff        | long      | Current variable difficulty            |
| coinbase_type       | long      | Coinbase template type (0-4)           |
| connection_duration | long      | Connection duration (seconds)          |

### `ocean_datum_shares` - Individual Share Submissions

| Column     | Type      | Description                    |
| ---------- | --------- | ------------------------------ |
| timestamp  | timestamp | Submission time                |
| worker     | symbol    | Worker/miner ID                |
| ip         | symbol    | Machine IP address             |
| status     | symbol    | accepted/rejected              |
| difficulty | long      | Share difficulty               |
| reason     | symbol    | Rejection reason (if rejected) |

## Example Queries

**Current hashrate by worker:**

```sql
SELECT worker, avg(calculated_th) as hashrate_th
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('h', -1, now())
GROUP BY worker;
```

**Pool total over time:**

```sql
SELECT timestamp, sum(calculated_th) as total_th
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('h', -24, now())
SAMPLE BY 5m;
```

**Worker activity:**

```sql
SELECT worker,
       avg(shares_per_min) as avg_shares_per_min,
       max(uptime_seconds) / 3600 as hours_active
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('h', -24, now())
GROUP BY worker;
```

**Mining software analysis:**

```sql
SELECT user_agent,
       count(distinct worker) as miners,
       avg(calculated_th) as avg_hashrate_th,
       avg(current_diff) as avg_difficulty
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('h', -1, now())
GROUP BY user_agent
ORDER BY miners DESC;
```

**Share rejection rates:**

```sql
SELECT worker,
       count() filter (where status = 'rejected') * 100.0 / count() as reject_rate
FROM ocean_datum_shares
WHERE timestamp > dateadd('h', -1, now())
GROUP BY worker;
```

## Grafana Integration

Add QuestDB as PostgreSQL data source:

- Host: `localhost:8812`
- Database: `qdb`
- SSL: disable

Sample dashboard query:

```sql
SELECT $__time(timestamp), sum(calculated_th) as "Total Hashrate (TH/s)"
FROM ocean_datum_hashrate
WHERE $__timeFilter(timestamp)
SAMPLE BY $__interval;
```

## Performance & Storage

- **CPU Impact**: <1% overhead
- **Memory**: ~50MB for metric buffering
- **Storage**: ~100MB/month per 10 miners
- **Network**: TCP packets (reliable delivery)

## Troubleshooting

**Test connectivity:**

```bash
nc -v -w 3 localhost 9009
```

**View logs:**

```bash
tail -f datum_gateway.log | grep -i questdb
```

**Common issues:**

- No data: Check firewall for TCP port 9009
- High memory: Reduce `batch_size` or increase `flush_interval_ms`
- Missing hashrate: Verify miners are submitting shares

## Security

- Data sent via TCP (unencrypted) to QuestDB
- Worker IDs and IP addresses are stored
- Secure at network level if privacy needed
- No authentication required for InfluxDB Line Protocol

## Files Added

- `src/datum_questdb.c/h` - QuestDB integration
- `src/datum_metrics_collector.c/h` - Mining metrics collection
- `doc/questdb_setup.sql` - Verification queries

## Support

For issues:

1. Check logs: `grep -i questdb datum_gateway.log`
2. Verify QuestDB tables exist and have data
3. Test network connectivity to TCP port 9009
4. Include configuration and QuestDB version in reports

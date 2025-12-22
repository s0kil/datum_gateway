# OCEAN DATUM Gateway - QuestDB Integration

Real-time hashrate monitoring and analytics for Datum

## Quick Start

### 1. Install QuestDB

```bash
docker run -d -p 9000:9000 -p 9009:9009 \
  -v questdb_data:/var/lib/questdb \
  --name questdb questdb/questdb:latest
```

### 2. Configure DATUM Gateway

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

Access QuestDB console: http://localhost:9000

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

- Host: `localhost:8812` (QuestDB PostgreSQL wire protocol)
- Database: `qdb`
- SSL: disable

```sql
SELECT $__time(timestamp), sum(calculated_th) as "Total Hashrate (TH/s)"
FROM ocean_datum_hashrate
WHERE $__timeFilter(timestamp)
SAMPLE BY $__interval;
```

## Production Deployment Checklist

### Pre-deployment

- [ ] QuestDB instance sized appropriately (min 4GB RAM for 1000+ workers)
- [ ] Network connectivity verified between DATUM Gateway and QuestDB
- [ ] Backup strategy configured for QuestDB data
- [ ] Monitoring alerts configured for QuestDB health
- [ ] Data retention policy defined and configured

### Configuration Limits

- **batch_size**: 10-10000 (recommended: 100-500)
- **flush_interval_ms**: 1000-60000 (recommended: 5000-10000)
- **collection_interval_sec**: 10-300 (recommended: 30-60)
- **hashrate_window_sec**: 60-3600 (recommended: 300)

### Resource Limits

- **MAX_WORKERS**: 10000 (hardcoded limit to prevent memory overflow)
- **Worker cleanup**: Automatic after 1 hour of inactivity
- **Aggressive cleanup**: Triggered at 5 minutes when approaching limits

## Performance & Storage

- **CPU Impact**: <1% overhead for up to 1000 workers
- **Memory**: ~50MB base + ~10KB per active worker
- **Storage**: ~100MB/month per 10 miners
- **Network**: TCP packets with automatic reconnection
- **Resilience**: Exponential backoff reconnection (1s, 2s, 4s... up to 60s)

## Performance Tuning

| Deployment Size | batch_size | flush_interval_ms | collection_interval_sec |
| --------------- | ---------- | ----------------- | ----------------------- |
| Small (<100)    | 50         | 10000             | 60                      |
| Medium (100-1K) | 100        | 5000              | 30                      |
| Large (>1K)     | 500        | 5000              | 30                      |

## Monitoring & Retention

**System Health Check:**

```sql
SELECT count(distinct worker) as active_workers,
       sum(calculated_th) as total_hashrate,
       max(timestamp) as last_update
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('m', -5, now());
```

**Recommended Retention:**

- Raw metrics: 7-30 days
- Hourly aggregates: 90 days
- Daily aggregates: 1 year

## Troubleshooting

| Issue                | Cause                 | Solution                                             |
| -------------------- | --------------------- | ---------------------------------------------------- |
| High hashrate values | Fixed time window bug | Fixed with dual-buffer implementation                |
| Connection failures  | Network/firewall      | Check `nc -v localhost 9009`, automatic reconnection |
| Memory growth        | Inactive workers      | Auto-cleanup after 1 hour (10K worker limit)         |
| Missing data         | Buffer flush errors   | Reduce batch_size or flush_interval_ms               |

**Debug Commands:**

```bash
# Test connectivity
nc -v -w 3 localhost 9009

# Check status
curl http://localhost:8080/api/questdb/stats

# View logs
tail -f datum_gateway.log | grep -i questdb

# Verify data
echo "SELECT count(*) FROM ocean_datum_hashrate WHERE timestamp > dateadd('m', -5, now());" | curl -G localhost:9000/exec --data-urlencode query@-
```

## Notes

- **Security**: Unencrypted TCP connection (secure at network level if needed)
- **Performance**: <1% CPU overhead, ~50MB + 10KB per worker memory usage
- **Limits**: 10K workers max, automatic cleanup, exponential backoff reconnection
- **Files**: `src/datum_questdb.c/h`, `src/datum_metrics_collector.c/h`

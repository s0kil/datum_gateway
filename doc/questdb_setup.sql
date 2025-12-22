-- OCEAN DATUM Gateway - QuestDB Table Setup Verification
-- 
-- This file contains SQL queries to verify that the OCEAN DATUM tables
-- are properly created and receiving data in QuestDB.
--
-- Run these queries in the QuestDB Web Console at http://localhost:9000

-- Check if tables exist and show their structure
SHOW TABLES;

-- Verify ocean_datum_hashrate table structure
-- This table stores periodic hashrate measurements
SELECT * FROM ocean_datum_hashrate LIMIT 0;

-- Verify ocean_datum_shares table structure  
-- This table stores individual share submissions
SELECT * FROM ocean_datum_shares LIMIT 0;

-- Check recent hashrate data (last hour)
SELECT 
    timestamp,
    worker,
    ip,
    user_agent,
    calculated_th,
    shares_accepted,
    shares_rejected,
    current_diff,
    uptime_seconds
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('h', -1, now())
ORDER BY timestamp DESC
LIMIT 10;

-- Check recent share submissions (last hour)
SELECT 
    timestamp,
    worker,
    ip,
    status,
    difficulty,
    reason
FROM ocean_datum_shares  
WHERE timestamp > dateadd('h', -1, now())
ORDER BY timestamp DESC
LIMIT 10;

-- Summary statistics for all workers
SELECT 
    worker,
    count() as data_points,
    avg(calculated_th) as avg_hashrate_th,
    sum(shares_accepted) as total_accepted,
    sum(shares_rejected) as total_rejected,
    avg(current_diff) as avg_difficulty,
    max(timestamp) as last_seen
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('d', -1, now())
GROUP BY worker
ORDER BY avg_hashrate_th DESC;

-- Pool-wide statistics
SELECT 
    count(DISTINCT worker) as active_workers,
    sum(calculated_th) as total_hashrate_th,
    avg(calculated_th) as avg_worker_hashrate_th,
    sum(shares_accepted + shares_rejected) as total_shares
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('m', -5, now());

-- Identify workers with low hashrate
SELECT 
    worker,
    avg(calculated_th) as avg_calculated_th,
    avg(shares_per_min) as avg_shares_per_min,
    avg(current_diff) as avg_difficulty,
    count() as samples,
    max(uptime_seconds) / 3600 as max_uptime_hours
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('h', -1, now())
    AND calculated_th > 0
GROUP BY worker
HAVING avg(calculated_th) < 1.0  -- Less than 1 TH/s
ORDER BY avg_calculated_th ASC;

-- Share rejection analysis
SELECT 
    worker,
    status,
    count() as share_count,
    sum(difficulty) as total_difficulty
FROM ocean_datum_shares
WHERE timestamp > dateadd('h', -1, now())
GROUP BY worker, status
ORDER BY worker, status;

-- Hourly hashrate trends
SELECT 
    timestamp,
    sum(calculated_th) as total_pool_hashrate_th,
    count(DISTINCT worker) as active_workers
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('d', -7, now())
SAMPLE BY 1h
ORDER BY timestamp DESC;
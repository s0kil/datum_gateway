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
    reported_th,
    calculated_th,
    efficiency,
    shares_accepted,
    shares_rejected
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
    avg(reported_th) as avg_reported_th,
    avg(efficiency) as avg_efficiency,
    sum(shares_accepted) as total_accepted,
    sum(shares_rejected) as total_rejected,
    max(timestamp) as last_seen
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('d', -1, now())
GROUP BY worker
ORDER BY avg_hashrate_th DESC;

-- Pool-wide statistics
SELECT 
    count(DISTINCT worker) as active_workers,
    sum(calculated_th) as total_hashrate_th,
    avg(efficiency) as pool_avg_efficiency
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('m', -5, now());

-- Identify workers with efficiency issues
SELECT 
    worker,
    avg(efficiency) as avg_efficiency,
    avg(calculated_th) as avg_calculated_th,
    avg(reported_th) as avg_reported_th,
    count() as samples
FROM ocean_datum_hashrate
WHERE timestamp > dateadd('h', -1, now())
    AND reported_th > 0
GROUP BY worker
HAVING avg(efficiency) < 0.9  -- Less than 90% efficiency
ORDER BY avg_efficiency ASC;

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
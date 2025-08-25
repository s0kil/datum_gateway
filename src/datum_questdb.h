/*
 *
 * DATUM Gateway - QuestDB Integration
 * Time series hashrate data collection and storage
 *
 * This file is part of OCEAN's Bitcoin mining decentralization
 * project, DATUM.
 *
 * https://ocean.xyz
 *
 * ---
 *
 * Copyright (c) 2024-2025 Bitcoin Ocean, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#ifndef _DATUM_QUESTDB_H_
#define _DATUM_QUESTDB_H_

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

// QuestDB configuration
typedef struct {
    bool enabled;
    char host[256];
    uint16_t ilp_port;  // InfluxDB Line Protocol port (default: 9009)
    uint32_t batch_size;
    uint32_t flush_interval_ms;
} T_DATUM_QUESTDB_CONFIG;

// Hashrate data point
typedef struct {
    uint64_t timestamp_ns;
    char worker_id[128];
    char machine_ip[64];
    double calculated_hashrate_th; // Pool calculated hashrate in TH/s
    uint64_t shares_accepted;
    uint64_t shares_rejected;
    uint64_t difficulty_accepted;
    uint64_t difficulty_rejected;
    
    // Available metrics from existing data
    double shares_per_min;        // Shares submitted per minute
    uint64_t uptime_seconds;      // Worker uptime since first share
    uint64_t last_share_delay_ms; // Time since last share submission
    
    // Stratum client metrics (from miner connection data)
    uint64_t current_diff;        // Current variable difficulty
    char user_agent[128];         // Mining software (e.g., "xminer-1.2.6")
    uint32_t coinbase_type;       // Coinbase template selection (0-4)
    uint64_t connection_duration; // How long connected (seconds)
} T_DATUM_HASHRATE_METRIC;


// Initialize QuestDB connection
bool datum_questdb_init(const T_DATUM_QUESTDB_CONFIG *config);

// Record hashrate metric to ocean_datum_hashrate table
void datum_questdb_record_hashrate(const T_DATUM_HASHRATE_METRIC *metric);

// Record share submission to ocean_datum_shares table
void datum_questdb_record_share(
    const char *worker_id,
    const char *machine_ip,
    uint64_t difficulty,
    bool accepted,
    const char *reject_reason
);


// Flush pending metrics to QuestDB
void datum_questdb_flush(void);

// Shutdown QuestDB connection
void datum_questdb_shutdown(void);

// Get statistics
void datum_questdb_get_stats(
    uint64_t *metrics_sent,
    uint64_t *metrics_failed,
    uint64_t *metrics_pending
);

// Check if QuestDB connection is healthy
bool datum_questdb_is_healthy(void);

#endif /* _DATUM_QUESTDB_H_ */
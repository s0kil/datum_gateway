/*
 *
 * DATUM Gateway - Metrics Collector
 * Collects and aggregates mining metrics and performance data
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

#ifndef _DATUM_METRICS_COLLECTOR_H_
#define _DATUM_METRICS_COLLECTOR_H_

#include <stdint.h>
#include <stdbool.h>
#include "datum_stratum.h"

// Initialize metrics collector
bool datum_metrics_collector_init(void);

// Called when a share is submitted
void datum_metrics_on_share_submit(
    const char *worker_id,
    const char *machine_ip,
    uint64_t difficulty,
    bool accepted,
    const char *reject_reason,
    uint64_t current_diff,
    const char *user_agent,
    uint32_t coinbase_type,
    uint64_t connect_time_ms
);

// Called periodically to collect and store mining metrics
void datum_metrics_collect_metrics(void);

// Get aggregated hashrate for a specific worker
double datum_metrics_get_worker_hashrate(const char *worker_id);

// Get total pool hashrate
double datum_metrics_get_pool_hashrate(void);

// Shutdown metrics collector
void datum_metrics_collector_shutdown(void);

// Start collector thread
void datum_metrics_collector_start(void);

#endif /* _DATUM_METRICS_COLLECTOR_H_ */
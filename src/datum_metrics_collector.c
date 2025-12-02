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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>

#include "datum_metrics_collector.h"
#include "datum_questdb.h"
#include "datum_logger.h"
#include "datum_utils.h"
#include "datum_sockets.h"
#include "datum_api.h"
#include "datum_conf.h"

#define WORKER_INACTIVE_TIMEOUT_MS (3600 * 1000)  // 1 hour inactive timeout
#define CLEANUP_INTERVAL_SEC 300    // Cleanup inactive workers every 5 minutes
#define STAT_CYCLE_MS 60000  // 60 second cycle for buffer swapping, same as dashboard
#define MAX_WORKERS 10000  // Maximum workers to track (configurable via config file)
#define AGGRESSIVE_CLEANUP_TIMEOUT_MS (300 * 1000)  // 5 minutes for aggressive cleanup

typedef struct worker_stats {
    char worker_id[128];
    char machine_ip[64];
    uint64_t shares_accepted;
    uint64_t shares_rejected;
    uint64_t difficulty_accepted;
    uint64_t difficulty_rejected;
    uint64_t last_share_time_ms;
    double reported_hashrate_th;
    double calculated_hashrate_th;
    uint64_t first_seen_ms;

    // Dual-buffer stats for accurate hashrate calculation
    uint8_t active_index;  // 0 or 1, indicates which buffer is active
    uint64_t diff_accepted_buffer[2];  // Two buffers for difficulty accepted
    uint64_t last_swap_tsms;  // Timestamp of last buffer swap
    uint64_t last_swap_ms;    // Elapsed time for the last completed buffer

    // Stratum client data
    uint64_t current_diff;
    char user_agent[128];
    uint32_t coinbase_type;
    uint64_t connect_time_ms;

    struct worker_stats *next;
} T_WORKER_STATS;

typedef struct {
    pthread_mutex_t mutex;
    pthread_t collector_thread;
    bool running;
    T_WORKER_STATS *workers;
    uint32_t worker_count;
    uint32_t total_workers_seen;  // Total workers ever seen
    uint32_t workers_cleaned;      // Total workers cleaned up
    uint64_t last_collection_time_ms;
    uint64_t last_cleanup_time_ms;
} T_METRICS_COLLECTOR_STATE;

static T_METRICS_COLLECTOR_STATE *g_collector = NULL;
static pthread_mutex_t g_init_mutex = PTHREAD_MUTEX_INITIALIZER;

// Forward declarations
static void *datum_metrics_collector_thread(void *arg);
static T_WORKER_STATS *find_or_create_worker(const char *worker_id, const char *machine_ip);
static void calculate_hashrates(void);
static void cleanup_inactive_workers(uint64_t timeout_ms);
static void free_worker_stats(T_WORKER_STATS *worker);

bool datum_metrics_collector_init(void) {
    pthread_mutex_lock(&g_init_mutex);

    if (g_collector) {
        DLOG_WARN("Metrics collector already initialized");
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }

    // Validate configuration
    if (datum_config.metrics_collector.collection_interval_sec < 10 || 
        datum_config.metrics_collector.collection_interval_sec > 300) {
        DLOG_ERROR("Invalid collection_interval_sec: %u (must be 10-300)",
                   datum_config.metrics_collector.collection_interval_sec);
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }
    
    if (datum_config.metrics_collector.hashrate_window_sec < 60 || 
        datum_config.metrics_collector.hashrate_window_sec > 3600) {
        DLOG_ERROR("Invalid hashrate_window_sec: %u (must be 60-3600)",
                   datum_config.metrics_collector.hashrate_window_sec);
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }
    
    g_collector = calloc(1, sizeof(T_METRICS_COLLECTOR_STATE));
    if (!g_collector) {
        DLOG_ERROR("Failed to allocate metrics collector state");
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }

    if (pthread_mutex_init(&g_collector->mutex, NULL) != 0) {
        DLOG_ERROR("Failed to initialize collector mutex");
        free(g_collector);
        g_collector = NULL;
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }

    g_collector->running = true;
    g_collector->last_collection_time_ms = current_time_millis();
    g_collector->last_cleanup_time_ms = current_time_millis();

    pthread_mutex_unlock(&g_init_mutex);

    DLOG_INFO("Metrics collector initialized");
    return true;
}

void datum_metrics_collector_start(void) {
    if (!g_collector) {
        DLOG_ERROR("Metrics collector not initialized");
        return;
    }

    if (pthread_create(&g_collector->collector_thread, NULL, datum_metrics_collector_thread, NULL) != 0) {
        DLOG_ERROR("Failed to create collector thread");
        return;
    }

    DLOG_INFO("Metrics collector thread started");
}

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
) {
    if (!g_collector || !worker_id) {
        return;
    }

    pthread_mutex_lock(&g_collector->mutex);

    T_WORKER_STATS *worker = find_or_create_worker(worker_id, machine_ip);
    if (worker) {
        uint64_t current_ms = current_time_millis();
        
        if (accepted) {
            worker->shares_accepted++;
            worker->difficulty_accepted += difficulty;
            
            // Add to the active buffer (using inverted index like dashboard)
            worker->diff_accepted_buffer[worker->active_index ? 1 : 0] += difficulty;
        } else {
            worker->shares_rejected++;
            worker->difficulty_rejected += difficulty;
        }

        worker->last_share_time_ms = current_ms;
        
        // Check if it's time to swap buffers (60 second cycle)
        if (current_ms >= (worker->last_swap_tsms + STAT_CYCLE_MS)) {
            worker->last_swap_ms = current_ms - worker->last_swap_tsms;
            worker->last_swap_tsms = current_ms;
            
            // Swap buffers and clear the new active one
            if (worker->active_index) {
                worker->active_index = 0;
                worker->diff_accepted_buffer[0] = 0;
            } else {
                worker->active_index = 1;
                worker->diff_accepted_buffer[1] = 0;
            }
        }

        // Update stratum client data
        worker->current_diff = current_diff;
        worker->coinbase_type = coinbase_type;
        worker->connect_time_ms = connect_time_ms;
        if (user_agent && strlen(user_agent) > 0) {
            strncpy(worker->user_agent, user_agent, sizeof(worker->user_agent) - 1);
            worker->user_agent[sizeof(worker->user_agent) - 1] = '\0';
        }

    }

    pthread_mutex_unlock(&g_collector->mutex);

    // Also record to QuestDB immediately for share-level tracking
    datum_questdb_record_share(worker_id, machine_ip, difficulty, accepted, reject_reason);
}

static T_WORKER_STATS *find_or_create_worker(const char *worker_id, const char *machine_ip) {
    T_WORKER_STATS *worker = g_collector->workers;

    // Search for existing worker
    while (worker) {
        if (strcmp(worker->worker_id, worker_id) == 0) {
            // Update IP if changed
            if (machine_ip && strcmp(worker->machine_ip, machine_ip) != 0) {
                strncpy(worker->machine_ip, machine_ip, sizeof(worker->machine_ip) - 1);
                worker->machine_ip[sizeof(worker->machine_ip) - 1] = '\0';
            }
            return worker;
        }
        worker = worker->next;
    }

    // Check if we've reached the worker limit
    if (g_collector->worker_count >= MAX_WORKERS) {
        DLOG_WARN("Worker limit reached (%u workers), performing aggressive cleanup", MAX_WORKERS);
        
        // Try aggressive cleanup first
        cleanup_inactive_workers(AGGRESSIVE_CLEANUP_TIMEOUT_MS);
        
        if (g_collector->worker_count >= MAX_WORKERS) {
            // Still at limit, remove oldest inactive worker
            T_WORKER_STATS *oldest = NULL;
            T_WORKER_STATS *oldest_prev = NULL;
            T_WORKER_STATS *curr = g_collector->workers;
            T_WORKER_STATS *prev = NULL;
            uint64_t oldest_time = UINT64_MAX;
            
            while (curr) {
                if (curr->last_share_time_ms < oldest_time) {
                    oldest_time = curr->last_share_time_ms;
                    oldest = curr;
                    oldest_prev = prev;
                }
                prev = curr;
                curr = curr->next;
            }
            
            if (oldest) {
                DLOG_WARN("Removing oldest worker to make room: %s (last seen: %llu ms ago)",
                          oldest->worker_id, 
                          (unsigned long long)(current_time_millis() - oldest->last_share_time_ms));
                
                if (oldest_prev) {
                    oldest_prev->next = oldest->next;
                } else {
                    g_collector->workers = oldest->next;
                }
                
                free_worker_stats(oldest);
                g_collector->worker_count--;
            } else {
                DLOG_ERROR("Cannot create new worker - at limit and unable to free space");
                return NULL;
            }
        }
    }
    
    // Create new worker
    worker = calloc(1, sizeof(T_WORKER_STATS));
    if (!worker) {
        DLOG_ERROR("Failed to allocate worker stats");
        return NULL;
    }

    strncpy(worker->worker_id, worker_id, sizeof(worker->worker_id) - 1);
    worker->worker_id[sizeof(worker->worker_id) - 1] = '\0';

    if (machine_ip) {
        strncpy(worker->machine_ip, machine_ip, sizeof(worker->machine_ip) - 1);
        worker->machine_ip[sizeof(worker->machine_ip) - 1] = '\0';
    }

    worker->first_seen_ms = current_time_millis();
    worker->last_share_time_ms = worker->first_seen_ms;
    
    // Initialize dual-buffer stats
    worker->active_index = 0;
    worker->diff_accepted_buffer[0] = 0;
    worker->diff_accepted_buffer[1] = 0;
    worker->last_swap_tsms = worker->first_seen_ms;
    worker->last_swap_ms = 0;

    // Add to list
    worker->next = g_collector->workers;
    g_collector->workers = worker;
    g_collector->worker_count++;
    g_collector->total_workers_seen++;

    DLOG_DEBUG("Created new worker stats for: %s (total seen: %u)",
               worker_id, g_collector->total_workers_seen);

    return worker;
}

static void cleanup_inactive_workers(uint64_t timeout_ms) {
    if (!g_collector) {
        return;
    }

    uint64_t current_time = current_time_millis();
    T_WORKER_STATS *worker = g_collector->workers;
    T_WORKER_STATS *prev = NULL;
    uint32_t cleaned = 0;

    while (worker) {
        T_WORKER_STATS *next = worker->next;

        // Check if worker is inactive
        if (current_time - worker->last_share_time_ms > timeout_ms) {
            // Remove inactive worker
            if (prev) {
                prev->next = next;
            } else {
                g_collector->workers = next;
            }

            DLOG_DEBUG("Removing inactive worker: %s (last seen: %llu ms ago)",
                      worker->worker_id,
                      (unsigned long long)(current_time - worker->last_share_time_ms));

            free_worker_stats(worker);
            g_collector->worker_count--;
            g_collector->workers_cleaned++;
            cleaned++;

            worker = next;
        } else {
            prev = worker;
            worker = next;
        }
    }

    if (cleaned > 0) {
        DLOG_INFO("Cleaned up %u inactive workers (total cleaned: %u, active: %u)",
                  cleaned, g_collector->workers_cleaned, g_collector->worker_count);
    }

    g_collector->last_cleanup_time_ms = current_time;
}

static void free_worker_stats(T_WORKER_STATS *worker) {
    if (worker) {
        // Could add additional cleanup here if needed
        free(worker);
    }
}

void datum_metrics_collect_metrics(void) {
    if (!g_collector) {
        return;
    }

    pthread_mutex_lock(&g_collector->mutex);

    // Calculate hashrates for all workers
    calculate_hashrates();

    // Get current timestamp
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    uint64_t timestamp_ns = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;

    // Check if QuestDB is initialized and healthy
    bool questdb_healthy = datum_questdb_is_healthy();
    static uint32_t unhealthy_count = 0;
    static uint32_t healthy_collections = 0;
    static bool was_healthy = false;

    if (!questdb_healthy) {
        unhealthy_count++;
        // Log less frequently to reduce spam - every 60 seconds (assuming 30s collection interval)
        if (unhealthy_count == 1 || (unhealthy_count % 2 == 0)) {
            DLOG_WARN("QuestDB unhealthy (count: %u), metrics will be collected when connection is restored",
                     unhealthy_count);
        }
        // Still calculate hashrates locally even if we can't send to QuestDB
        // This keeps worker stats up to date for when connection is restored
        pthread_mutex_unlock(&g_collector->mutex);
        return;
    }

    // Log recovery or periodic healthy status
    if (!was_healthy) {
        DLOG_INFO("QuestDB connection restored - resuming metrics collection");
        unhealthy_count = 0;
    }
    was_healthy = true;
    healthy_collections++;
    if (healthy_collections % 20 == 0) {
        DLOG_DEBUG("QuestDB healthy - metrics collection #%u", healthy_collections);
    }

    // Collect and send metrics for each worker
    T_WORKER_STATS *worker = g_collector->workers;
    uint32_t active_workers = 0;

    while (worker) {
        // Only send metrics for workers with recent activity
        uint64_t time_since_share = current_time_millis() - worker->last_share_time_ms;
        if (time_since_share < datum_config.metrics_collector.hashrate_window_sec * 1000) {
            // Calculate enhanced metrics
            uint64_t uptime_ms = current_time_millis() - worker->first_seen_ms;
            uint64_t collection_window_ms = datum_config.metrics_collector.collection_interval_sec * 1000;
            double shares_per_min = 0.0;

            // Calculate shares per minute over the collection interval
            if (collection_window_ms > 0) {
                uint64_t total_shares = worker->shares_accepted + worker->shares_rejected;
                shares_per_min = ((double)total_shares * 60000.0) / collection_window_ms;
            }

            uint64_t connection_duration_sec = (current_time_millis() - worker->connect_time_ms) / 1000;

            T_DATUM_HASHRATE_METRIC metric = {
                .timestamp_ns = timestamp_ns,
                .calculated_hashrate_th = worker->calculated_hashrate_th,
                .shares_accepted = worker->shares_accepted,
                .shares_rejected = worker->shares_rejected,
                .difficulty_accepted = worker->difficulty_accepted,
                .difficulty_rejected = worker->difficulty_rejected,

                // Available metrics from existing data
                .shares_per_min = shares_per_min,
                .uptime_seconds = uptime_ms / 1000,
                .last_share_delay_ms = time_since_share,

                // Stratum client metrics
                .current_diff = worker->current_diff,
                .coinbase_type = worker->coinbase_type,
                .connection_duration = connection_duration_sec
            };

            strncpy(metric.worker_id, worker->worker_id, sizeof(metric.worker_id) - 1);
            metric.worker_id[sizeof(metric.worker_id) - 1] = '\0';

            strncpy(metric.machine_ip, worker->machine_ip, sizeof(metric.machine_ip) - 1);
            metric.machine_ip[sizeof(metric.machine_ip) - 1] = '\0';

            strncpy(metric.user_agent, worker->user_agent, sizeof(metric.user_agent) - 1);
            metric.user_agent[sizeof(metric.user_agent) - 1] = '\0';

            // Send to QuestDB
            datum_questdb_record_hashrate(&metric);
            active_workers++;
        }

        worker = worker->next;
    }

    g_collector->last_collection_time_ms = current_time_millis();

    // Perform cleanup if it's time
    if (g_collector->last_collection_time_ms - g_collector->last_cleanup_time_ms >
        CLEANUP_INTERVAL_SEC * 1000) {
        cleanup_inactive_workers(WORKER_INACTIVE_TIMEOUT_MS);
    }

    pthread_mutex_unlock(&g_collector->mutex);

    DLOG_DEBUG("Collected hashrate metrics for %u active workers (total tracked: %u)",
               active_workers, g_collector->worker_count);
}

static void calculate_hashrates(void) {
    uint64_t current_time_ms = current_time_millis();
    
    T_WORKER_STATS *worker = g_collector->workers;
    while (worker) {
        // Use dual-buffer approach like dashboard
        // astat is the inverted index (the completed buffer we read from)
        uint8_t astat = worker->active_index ? 0 : 1;
        double hr = 0.0;
        
        // Calculate hashrate if we have data in the completed buffer
        if ((worker->last_swap_ms > 0) && (worker->diff_accepted_buffer[astat] > 0)) {
            // Use actual elapsed time for the completed buffer
            hr = ((double)worker->diff_accepted_buffer[astat] / 
                  (double)(worker->last_swap_ms / 1000.0)) * 0.004294967296;
        }
        
        // Only include if worker was active recently (within 180 seconds like dashboard)
        if (((double)(current_time_ms - worker->last_swap_tsms) / 1000.0) < 180.0) {
            worker->calculated_hashrate_th = hr;
        } else {
            worker->calculated_hashrate_th = 0.0;
        }

        worker = worker->next;
    }
}

double datum_metrics_get_worker_hashrate(const char *worker_id) {
    if (!g_collector || !worker_id) {
        return 0.0;
    }

    double hashrate = 0.0;

    pthread_mutex_lock(&g_collector->mutex);

    T_WORKER_STATS *worker = g_collector->workers;
    while (worker) {
        if (strcmp(worker->worker_id, worker_id) == 0) {
            hashrate = worker->calculated_hashrate_th;
            break;
        }
        worker = worker->next;
    }

    pthread_mutex_unlock(&g_collector->mutex);

    return hashrate;
}

double datum_metrics_get_pool_hashrate(void) {
    if (!g_collector) {
        return 0.0;
    }

    double total_hashrate = 0.0;

    pthread_mutex_lock(&g_collector->mutex);

    T_WORKER_STATS *worker = g_collector->workers;
    while (worker) {
        total_hashrate += worker->calculated_hashrate_th;
        worker = worker->next;
    }

    pthread_mutex_unlock(&g_collector->mutex);

    return total_hashrate;
}

static void *datum_metrics_collector_thread(void *arg) {
    DLOG_INFO("Metrics collector thread started");

    uint32_t collection_count = 0;

    while (g_collector && g_collector->running) {
        sleep(datum_config.metrics_collector.collection_interval_sec);

        if (g_collector && g_collector->running) {
            datum_metrics_collect_metrics();
            collection_count++;

            // Log statistics every 10 collections (5 minutes)
            if (collection_count % 10 == 0) {
                DLOG_INFO("Collector stats - Active workers: %u, Total seen: %u, Cleaned: %u",
                         g_collector->worker_count,
                         g_collector->total_workers_seen,
                         g_collector->workers_cleaned);
            }
        }
    }

    DLOG_INFO("Metrics collector thread exiting");
    return NULL;
}

void datum_metrics_collector_shutdown(void) {
    if (!g_collector) {
        return;
    }

    DLOG_INFO("Shutting down metrics collector");

    // Stop collector thread
    g_collector->running = false;
    pthread_join(g_collector->collector_thread, NULL);

    // Final collection
    datum_metrics_collect_metrics();

    // Cleanup
    pthread_mutex_lock(&g_collector->mutex);

    uint32_t freed_count = 0;
    T_WORKER_STATS *worker = g_collector->workers;
    while (worker) {
        T_WORKER_STATS *next = worker->next;
        free_worker_stats(worker);
        freed_count++;
        worker = next;
    }

    DLOG_INFO("Freed %u worker stats structures", freed_count);
    DLOG_INFO("Final stats - Total workers seen: %u, Total cleaned: %u",
              g_collector->total_workers_seen,
              g_collector->workers_cleaned);

    pthread_mutex_unlock(&g_collector->mutex);
    pthread_mutex_destroy(&g_collector->mutex);

    free(g_collector);
    g_collector = NULL;

    DLOG_INFO("Metrics collector shutdown complete");
}

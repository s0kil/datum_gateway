/*
 *
 * DATUM Gateway - QuestDB Integration
 * Time series hashrate data collection and storage using official QuestDB C client
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
#include <time.h>
#include <unistd.h>

#include <questdb/ingress/line_sender.h>

#include "datum_questdb.h"
#include "datum_logger.h"
#include "datum_utils.h"

// QuestDB connection and state management
typedef struct {
    T_DATUM_QUESTDB_CONFIG config;
    line_sender* sender;
    line_sender_buffer* buffer;
    pthread_mutex_t mutex;
    pthread_t flush_thread;
    bool running;
    bool is_healthy;
    
    // Connection resilience
    uint32_t reconnect_attempts;
    uint64_t last_reconnect_attempt_ms;
    uint32_t consecutive_failures;
    
    // Statistics
    uint64_t metrics_sent;
    uint64_t metrics_failed;
    uint64_t metrics_pending;
    uint64_t last_flush_time_ms;
    uint64_t buffer_flushes;
    uint64_t last_successful_flush_ms;
} T_DATUM_QUESTDB_STATE;

static T_DATUM_QUESTDB_STATE *g_questdb_state = NULL;
static pthread_mutex_t g_init_mutex = PTHREAD_MUTEX_INITIALIZER;

// Table and column name helpers (defined at runtime to avoid static initialization issues)
#define HASHRATE_TABLE_NAME "ocean_datum_hashrate"
#define SHARES_TABLE_NAME "ocean_datum_shares"

// Common columns
#define WORKER_COL_NAME "worker"
#define IP_COL_NAME "ip"

// Hashrate table columns
#define CALCULATED_TH_COL_NAME "calculated_th"
#define SHARES_ACCEPTED_COL_NAME "shares_accepted"
#define SHARES_REJECTED_COL_NAME "shares_rejected"
#define DIFF_ACCEPTED_COL_NAME "diff_accepted"
#define DIFF_REJECTED_COL_NAME "diff_rejected"
#define SHARES_PER_MIN_COL_NAME "shares_per_min"
#define UPTIME_SECONDS_COL_NAME "uptime_seconds"
#define LAST_SHARE_DELAY_COL_NAME "last_share_delay_ms"
#define CURRENT_DIFF_COL_NAME "current_diff"
#define USER_AGENT_COL_NAME "user_agent"
#define COINBASE_TYPE_COL_NAME "coinbase_type"
#define CONNECTION_DURATION_COL_NAME "connection_duration"

// Shares table columns  
#define STATUS_COL_NAME "status"
#define REASON_COL_NAME "reason"
#define DIFFICULTY_COL_NAME "difficulty"


// Forward declarations
static void *datum_questdb_flush_thread(void *arg);
static bool datum_questdb_flush_buffer(void);
static bool validate_config(const T_DATUM_QUESTDB_CONFIG *config);
static void handle_questdb_error(line_sender_error *err, const char *operation);
static bool reconnect_questdb(void);

bool datum_questdb_init(const T_DATUM_QUESTDB_CONFIG *config) {
    if (!config) {
        DLOG_ERROR("NULL configuration provided to QuestDB init");
        return false;
    }
    
    if (!config->enabled) {
        DLOG_INFO("QuestDB integration disabled");
        return true;
    }
    
    if (!validate_config(config)) {
        return false;
    }
    
    pthread_mutex_lock(&g_init_mutex);
    
    if (g_questdb_state) {
        DLOG_WARN("QuestDB already initialized");
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }
    
    g_questdb_state = calloc(1, sizeof(T_DATUM_QUESTDB_STATE));
    if (!g_questdb_state) {
        DLOG_ERROR("Failed to allocate QuestDB state");
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }
    
    // Copy configuration
    memcpy(&g_questdb_state->config, config, sizeof(T_DATUM_QUESTDB_CONFIG));
    
    // Create connection string
    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str), "tcp::addr=%s:%u;", config->host, config->ilp_port);
    
    DLOG_INFO("Initializing QuestDB connection: %s", conn_str);
    
    // Initialize QuestDB connection
    line_sender_error *err = NULL;
    line_sender_utf8 conf_utf8 = {.len = strlen(conn_str), .buf = conn_str};
    g_questdb_state->sender = line_sender_from_conf(conf_utf8, &err);
    if (!g_questdb_state->sender) {
        DLOG_ERROR("QuestDB connection FAILED to %s:%u", config->host, config->ilp_port);
        handle_questdb_error(err, "connection initialization");
        free(g_questdb_state);
        g_questdb_state = NULL;
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }
    
    DLOG_INFO("QuestDB connection SUCCESSFUL to %s:%u", config->host, config->ilp_port);
    
    // Create buffer
    g_questdb_state->buffer = line_sender_buffer_new_for_sender(g_questdb_state->sender);
    if (!g_questdb_state->buffer) {
        DLOG_ERROR("Failed to create QuestDB buffer");
        line_sender_close(g_questdb_state->sender);
        free(g_questdb_state);
        g_questdb_state = NULL;
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }
    
    // Initialize mutex
    if (pthread_mutex_init(&g_questdb_state->mutex, NULL) != 0) {
        DLOG_ERROR("Failed to initialize mutex");
        line_sender_buffer_free(g_questdb_state->buffer);
        line_sender_close(g_questdb_state->sender);
        free(g_questdb_state);
        g_questdb_state = NULL;
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }
    
    // Initialize state
    g_questdb_state->is_healthy = true;
    g_questdb_state->running = true;
    
    // Start flush thread
    if (pthread_create(&g_questdb_state->flush_thread, NULL, datum_questdb_flush_thread, NULL) != 0) {
        DLOG_ERROR("Failed to create flush thread");
        pthread_mutex_destroy(&g_questdb_state->mutex);
        line_sender_buffer_free(g_questdb_state->buffer);
        line_sender_close(g_questdb_state->sender);
        free(g_questdb_state);
        g_questdb_state = NULL;
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }
    
    pthread_mutex_unlock(&g_init_mutex);
    
    DLOG_INFO("QuestDB integration initialized - host: %s:%u, flush_interval: %ums",
              config->host, config->ilp_port, config->flush_interval_ms);
    
    return true;
}

static bool validate_config(const T_DATUM_QUESTDB_CONFIG *config) {
    if (!config->host[0]) {
        DLOG_ERROR("Invalid QuestDB configuration: empty hostname");
        return false;
    }
    
    if (config->ilp_port == 0 || config->ilp_port > 65535) {
        DLOG_ERROR("Invalid ILP port: %u (must be 1-65535)", config->ilp_port);
        return false;
    }
    
    // Validate batch size
    if (config->batch_size < 10 || config->batch_size > 10000) {
        DLOG_ERROR("Invalid batch_size: %u (must be 10-10000)", config->batch_size);
        return false;
    }
    
    // Validate flush interval
    if (config->flush_interval_ms < 1000 || config->flush_interval_ms > 60000) {
        DLOG_ERROR("Invalid flush_interval_ms: %u (must be 1000-60000)", config->flush_interval_ms);
        return false;
    }
    
    return true;
}

static void handle_questdb_error(line_sender_error *err, const char *operation) {
    if (err) {
        size_t msg_len;
        const char* msg = line_sender_error_msg(err, &msg_len);
        DLOG_ERROR("QuestDB %s error: %.*s", operation, (int)msg_len, msg);
        line_sender_error_free(err);
        
        if (g_questdb_state) {
            g_questdb_state->is_healthy = false;
            g_questdb_state->metrics_failed++;
            g_questdb_state->consecutive_failures++;
            
            // Trigger reconnection after multiple failures
            if (g_questdb_state->consecutive_failures >= 3) {
                DLOG_WARN("Multiple QuestDB failures detected, will attempt reconnection");
            }
        }
    }
}

void datum_questdb_record_hashrate(const T_DATUM_HASHRATE_METRIC *metric) {
    if (!g_questdb_state || !metric || !g_questdb_state->is_healthy) {
        return;
    }
    
    pthread_mutex_lock(&g_questdb_state->mutex);
    
    line_sender_error *err = NULL;
    
    // Start building the record
    line_sender_table_name table_name = QDB_TABLE_NAME_LITERAL(HASHRATE_TABLE_NAME);
    if (!line_sender_buffer_table(g_questdb_state->buffer, table_name, &err)) {
        handle_questdb_error(err, "setting table name");
        goto cleanup;
    }
    
    // Add symbols (tags)
    line_sender_utf8 worker_utf8 = {.len = strlen(metric->worker_id), .buf = metric->worker_id};
    line_sender_utf8 ip_utf8 = {.len = strlen(metric->machine_ip), .buf = metric->machine_ip};
    line_sender_utf8 user_agent_utf8 = {.len = strlen(metric->user_agent), .buf = metric->user_agent};
    
    line_sender_column_name worker_col = QDB_COLUMN_NAME_LITERAL(WORKER_COL_NAME);
    line_sender_column_name ip_col = QDB_COLUMN_NAME_LITERAL(IP_COL_NAME);
    line_sender_column_name user_agent_col = QDB_COLUMN_NAME_LITERAL(USER_AGENT_COL_NAME);
    
    if (!line_sender_buffer_symbol(g_questdb_state->buffer, worker_col, worker_utf8, &err) ||
        !line_sender_buffer_symbol(g_questdb_state->buffer, ip_col, ip_utf8, &err) ||
        !line_sender_buffer_symbol(g_questdb_state->buffer, user_agent_col, user_agent_utf8, &err)) {
        handle_questdb_error(err, "adding symbols");
        goto cleanup;
    }
    
    // Add numeric columns
    line_sender_column_name calculated_th_col = QDB_COLUMN_NAME_LITERAL(CALCULATED_TH_COL_NAME);
    line_sender_column_name shares_accepted_col = QDB_COLUMN_NAME_LITERAL(SHARES_ACCEPTED_COL_NAME);
    line_sender_column_name shares_rejected_col = QDB_COLUMN_NAME_LITERAL(SHARES_REJECTED_COL_NAME);
    line_sender_column_name diff_accepted_col = QDB_COLUMN_NAME_LITERAL(DIFF_ACCEPTED_COL_NAME);
    line_sender_column_name diff_rejected_col = QDB_COLUMN_NAME_LITERAL(DIFF_REJECTED_COL_NAME);
    
    // Available metrics columns
    line_sender_column_name shares_per_min_col = QDB_COLUMN_NAME_LITERAL(SHARES_PER_MIN_COL_NAME);
    line_sender_column_name uptime_seconds_col = QDB_COLUMN_NAME_LITERAL(UPTIME_SECONDS_COL_NAME);
    line_sender_column_name last_share_delay_col = QDB_COLUMN_NAME_LITERAL(LAST_SHARE_DELAY_COL_NAME);
    
    // Stratum client metrics columns
    line_sender_column_name current_diff_col = QDB_COLUMN_NAME_LITERAL(CURRENT_DIFF_COL_NAME);
    line_sender_column_name coinbase_type_col = QDB_COLUMN_NAME_LITERAL(COINBASE_TYPE_COL_NAME);
    line_sender_column_name connection_duration_col = QDB_COLUMN_NAME_LITERAL(CONNECTION_DURATION_COL_NAME);
    
    if (!line_sender_buffer_column_f64(g_questdb_state->buffer, calculated_th_col, metric->calculated_hashrate_th, &err) ||
        !line_sender_buffer_column_i64(g_questdb_state->buffer, shares_accepted_col, metric->shares_accepted, &err) ||
        !line_sender_buffer_column_i64(g_questdb_state->buffer, shares_rejected_col, metric->shares_rejected, &err) ||
        !line_sender_buffer_column_i64(g_questdb_state->buffer, diff_accepted_col, metric->difficulty_accepted, &err) ||
        !line_sender_buffer_column_i64(g_questdb_state->buffer, diff_rejected_col, metric->difficulty_rejected, &err) ||
        // Available metrics
        !line_sender_buffer_column_f64(g_questdb_state->buffer, shares_per_min_col, metric->shares_per_min, &err) ||
        !line_sender_buffer_column_i64(g_questdb_state->buffer, uptime_seconds_col, metric->uptime_seconds, &err) ||
        !line_sender_buffer_column_i64(g_questdb_state->buffer, last_share_delay_col, metric->last_share_delay_ms, &err) ||
        // Stratum client metrics
        !line_sender_buffer_column_i64(g_questdb_state->buffer, current_diff_col, metric->current_diff, &err) ||
        !line_sender_buffer_column_i64(g_questdb_state->buffer, coinbase_type_col, metric->coinbase_type, &err) ||
        !line_sender_buffer_column_i64(g_questdb_state->buffer, connection_duration_col, metric->connection_duration, &err)) {
        handle_questdb_error(err, "adding columns");
        goto cleanup;
    }
    
    // Set timestamp and complete the record
    if (!line_sender_buffer_at_nanos(g_questdb_state->buffer, metric->timestamp_ns, &err)) {
        handle_questdb_error(err, "setting timestamp");
        goto cleanup;
    }
    
    g_questdb_state->metrics_pending++;
    
    // Check if we should flush based on batch size
    if (g_questdb_state->metrics_pending >= g_questdb_state->config.batch_size) {
        datum_questdb_flush_buffer();
    }
    
cleanup:
    pthread_mutex_unlock(&g_questdb_state->mutex);
}

void datum_questdb_record_share(
    const char *worker_id,
    const char *machine_ip,
    uint64_t difficulty,
    bool accepted,
    const char *reject_reason
) {
    if (!g_questdb_state || !worker_id || !g_questdb_state->is_healthy) {
        return;
    }
    
    pthread_mutex_lock(&g_questdb_state->mutex);
    
    line_sender_error *err = NULL;
    
    // Start building the record
    line_sender_table_name shares_table = QDB_TABLE_NAME_LITERAL(SHARES_TABLE_NAME);
    if (!line_sender_buffer_table(g_questdb_state->buffer, shares_table, &err)) {
        handle_questdb_error(err, "setting shares table");
        goto cleanup;
    }
    
    // Add symbols
    line_sender_utf8 worker_utf8 = {.len = strlen(worker_id), .buf = worker_id};
    line_sender_utf8 ip_utf8 = {.len = strlen(machine_ip ? machine_ip : "unknown"), .buf = machine_ip ? machine_ip : "unknown"};
    line_sender_utf8 status_utf8 = {.len = strlen(accepted ? "accepted" : "rejected"), .buf = accepted ? "accepted" : "rejected"};
    
    line_sender_column_name worker_col = QDB_COLUMN_NAME_LITERAL(WORKER_COL_NAME);
    line_sender_column_name ip_col = QDB_COLUMN_NAME_LITERAL(IP_COL_NAME);
    line_sender_column_name status_col = QDB_COLUMN_NAME_LITERAL(STATUS_COL_NAME);
    
    if (!line_sender_buffer_symbol(g_questdb_state->buffer, worker_col, worker_utf8, &err) ||
        !line_sender_buffer_symbol(g_questdb_state->buffer, ip_col, ip_utf8, &err) ||
        !line_sender_buffer_symbol(g_questdb_state->buffer, status_col, status_utf8, &err)) {
        handle_questdb_error(err, "adding share symbols");
        goto cleanup;
    }
    
    // Add reject reason if rejected
    if (!accepted && reject_reason) {
        line_sender_utf8 reason_utf8 = {.len = strlen(reject_reason), .buf = reject_reason};
        line_sender_column_name reason_col = QDB_COLUMN_NAME_LITERAL(REASON_COL_NAME);
        if (!line_sender_buffer_symbol(g_questdb_state->buffer, reason_col, reason_utf8, &err)) {
            handle_questdb_error(err, "adding reject reason");
            goto cleanup;
        }
    }
    
    // Add difficulty column
    line_sender_column_name difficulty_col = QDB_COLUMN_NAME_LITERAL(DIFFICULTY_COL_NAME);
    if (!line_sender_buffer_column_i64(g_questdb_state->buffer, difficulty_col, difficulty, &err)) {
        handle_questdb_error(err, "adding difficulty column");
        goto cleanup;
    }
    
    
    // Set timestamp and complete
    if (!line_sender_buffer_at_nanos(g_questdb_state->buffer, line_sender_now_nanos(), &err)) {
        handle_questdb_error(err, "setting share timestamp");
        goto cleanup;
    }
    
    g_questdb_state->metrics_pending++;
    
    // Flush immediately for shares (low latency)
    datum_questdb_flush_buffer();
    
cleanup:
    pthread_mutex_unlock(&g_questdb_state->mutex);
}

static bool datum_questdb_flush_buffer(void) {
    if (!g_questdb_state || g_questdb_state->metrics_pending == 0) {
        return true;
    }
    
    // Attempt reconnection if unhealthy and multiple failures
    if (!g_questdb_state->is_healthy && g_questdb_state->consecutive_failures >= 3) {
        if (reconnect_questdb()) {
            DLOG_INFO("QuestDB reconnection successful, retrying flush");
        } else {
            return false;
        }
    }
    
    line_sender_error *err = NULL;
    
    if (!line_sender_flush(g_questdb_state->sender, g_questdb_state->buffer, &err)) {
        handle_questdb_error(err, "flushing buffer");
        
        // Try reconnection on flush failure
        if (g_questdb_state->consecutive_failures >= 3) {
            reconnect_questdb();
        }
        return false;
    }
    
    // Update statistics on success
    g_questdb_state->metrics_sent += g_questdb_state->metrics_pending;
    g_questdb_state->metrics_pending = 0;
    g_questdb_state->last_flush_time_ms = current_time_millis();
    g_questdb_state->last_successful_flush_ms = g_questdb_state->last_flush_time_ms;
    g_questdb_state->buffer_flushes++;
    g_questdb_state->is_healthy = true;
    g_questdb_state->consecutive_failures = 0;
    
    return true;
}

void datum_questdb_flush(void) {
    if (!g_questdb_state) {
        return;
    }
    
    pthread_mutex_lock(&g_questdb_state->mutex);
    datum_questdb_flush_buffer();
    pthread_mutex_unlock(&g_questdb_state->mutex);
}

static void *datum_questdb_flush_thread(void *arg) {
    DLOG_DEBUG("QuestDB flush thread started");
    
    while (g_questdb_state && g_questdb_state->running) {
        usleep(g_questdb_state->config.flush_interval_ms * 1000);
        
        if (g_questdb_state && g_questdb_state->running) {
            datum_questdb_flush();
        }
    }
    
    DLOG_DEBUG("QuestDB flush thread exiting");
    return NULL;
}

void datum_questdb_shutdown(void) {
    if (!g_questdb_state) {
        return;
    }
    
    DLOG_INFO("Shutting down QuestDB integration");
    
    // Stop flush thread
    g_questdb_state->running = false;
    pthread_join(g_questdb_state->flush_thread, NULL);
    
    // Final flush
    pthread_mutex_lock(&g_questdb_state->mutex);
    datum_questdb_flush_buffer();
    pthread_mutex_unlock(&g_questdb_state->mutex);
    
    DLOG_INFO("QuestDB stats - sent: %llu, failed: %llu",
              (unsigned long long)g_questdb_state->metrics_sent,
              (unsigned long long)g_questdb_state->metrics_failed);
    
    // Cleanup
    pthread_mutex_destroy(&g_questdb_state->mutex);
    line_sender_buffer_free(g_questdb_state->buffer);
    line_sender_close(g_questdb_state->sender);
    
    free(g_questdb_state);
    g_questdb_state = NULL;
    
    DLOG_INFO("QuestDB integration shutdown complete");
}

void datum_questdb_get_stats(
    uint64_t *metrics_sent,
    uint64_t *metrics_failed,
    uint64_t *metrics_pending
) {
    if (!g_questdb_state) {
        if (metrics_sent) *metrics_sent = 0;
        if (metrics_failed) *metrics_failed = 0;
        if (metrics_pending) *metrics_pending = 0;
        return;
    }
    
    pthread_mutex_lock(&g_questdb_state->mutex);
    
    if (metrics_sent) *metrics_sent = g_questdb_state->metrics_sent;
    if (metrics_failed) *metrics_failed = g_questdb_state->metrics_failed;
    if (metrics_pending) *metrics_pending = g_questdb_state->metrics_pending;
    
    pthread_mutex_unlock(&g_questdb_state->mutex);
}

bool datum_questdb_is_healthy(void) {
    if (!g_questdb_state) {
        static uint32_t null_state_count = 0;
        null_state_count++;
        if (null_state_count == 1 || (null_state_count % 50 == 0)) {
            DLOG_WARN("QuestDB state is NULL - initialization may have failed (count: %u)", null_state_count);
        }
        return false;
    }
    
    if (!g_questdb_state->is_healthy) {
        static uint32_t unhealthy_state_count = 0;
        unhealthy_state_count++;
        if (unhealthy_state_count == 1 || (unhealthy_state_count % 50 == 0)) {
            DLOG_WARN("QuestDB state is unhealthy - connection or flush errors occurred (count: %u, failed: %llu)", 
                     unhealthy_state_count, (unsigned long long)g_questdb_state->metrics_failed);
        }
        return false;
    }
    
    return g_questdb_state->is_healthy;
}

static bool reconnect_questdb(void) {
    if (!g_questdb_state) {
        return false;
    }
    
    uint64_t current_ms = current_time_millis();
    
    // Implement exponential backoff
    uint64_t min_wait_ms = (1 << g_questdb_state->reconnect_attempts) * 1000; // 1s, 2s, 4s, 8s...
    if (min_wait_ms > 60000) min_wait_ms = 60000; // Cap at 60 seconds
    
    // Check if enough time has passed since last attempt
    if (current_ms - g_questdb_state->last_reconnect_attempt_ms < min_wait_ms) {
        return false; // Too soon to retry
    }
    
    g_questdb_state->last_reconnect_attempt_ms = current_ms;
    g_questdb_state->reconnect_attempts++;
    
    DLOG_INFO("Attempting QuestDB reconnection (attempt %u)", g_questdb_state->reconnect_attempts);
    
    // Close existing connection
    if (g_questdb_state->sender) {
        line_sender_close(g_questdb_state->sender);
        g_questdb_state->sender = NULL;
    }
    
    // Clear existing buffer
    if (g_questdb_state->buffer) {
        line_sender_buffer_free(g_questdb_state->buffer);
        g_questdb_state->buffer = NULL;
    }
    
    // Recreate connection
    char conn_str[512];
    snprintf(conn_str, sizeof(conn_str), "tcp::addr=%s:%u;", 
             g_questdb_state->config.host, g_questdb_state->config.ilp_port);
    
    line_sender_error *err = NULL;
    line_sender_utf8 conf_utf8 = {.len = strlen(conn_str), .buf = conn_str};
    g_questdb_state->sender = line_sender_from_conf(conf_utf8, &err);
    
    if (!g_questdb_state->sender) {
        DLOG_ERROR("QuestDB reconnection failed: %s:%u", 
                   g_questdb_state->config.host, g_questdb_state->config.ilp_port);
        handle_questdb_error(err, "reconnection");
        return false;
    }
    
    // Recreate buffer
    g_questdb_state->buffer = line_sender_buffer_new_for_sender(g_questdb_state->sender);
    if (!g_questdb_state->buffer) {
        DLOG_ERROR("Failed to recreate QuestDB buffer during reconnection");
        line_sender_close(g_questdb_state->sender);
        g_questdb_state->sender = NULL;
        return false;
    }
    
    // Reset state
    g_questdb_state->is_healthy = true;
    g_questdb_state->consecutive_failures = 0;
    g_questdb_state->reconnect_attempts = 0;
    g_questdb_state->metrics_pending = 0;
    
    DLOG_INFO("QuestDB reconnection successful");
    return true;
}



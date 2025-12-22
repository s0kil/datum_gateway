/*
 * DATUM Gateway - QuestDB Integration
 * Time series metrics collection using QuestDB ILP (InfluxDB Line Protocol)
 *
 * Copyright (c) 2024-2025 Bitcoin Ocean, LLC
 * https://ocean.xyz
 *
 * MIT License - See LICENSE file for details
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>

#include <questdb/ingress/line_sender.h>

#include "datum_questdb.h"
#include "datum_logger.h"
#include "datum_utils.h"

// Configuration constants
#define STARTUP_RETRIES         5
#define STARTUP_RETRY_DELAY_SEC 3
#define MAX_BACKOFF_MS          60000
#define RECONNECT_THRESHOLD     3
#define CONN_STR_SIZE           512

// Table names
#define TBL_HASHRATE "ocean_datum_hashrate"
#define TBL_SHARES   "ocean_datum_shares"

// Helper macros for QuestDB API - reduce boilerplate significantly
#define QDB_UTF8(s)     (line_sender_utf8){.len = strlen(s), .buf = (s)}
#define QDB_UTF8_OR(s, def) (line_sender_utf8){.len = strlen((s) ? (s) : (def)), .buf = (s) ? (s) : (def)}
#define QDB_TABLE(name) QDB_TABLE_NAME_LITERAL(name)
#define QDB_COL(name)   QDB_COLUMN_NAME_LITERAL(name)

// Check result and goto cleanup on failure
#define QDB_CHECK(call, op) do { \
    if (!(call)) { \
        log_and_handle_error(err, (op)); \
        goto cleanup; \
    } \
} while (0)

// State structure
typedef struct {
    T_DATUM_QUESTDB_CONFIG config;
    line_sender           *sender;
    line_sender_buffer    *buffer;
    pthread_mutex_t        mutex;
    pthread_t              flush_thread;

    // Atomics for lock-free reads
    atomic_bool            running;
    atomic_bool            healthy;

    // Reconnection state (protected by mutex)
    uint32_t               reconnect_attempts;
    uint64_t               last_reconnect_ms;
    uint32_t               consecutive_failures;

    // Statistics
    uint64_t               sent;
    uint64_t               failed;
    uint64_t               pending;
    uint64_t               flushes;
} questdb_state_t;

static questdb_state_t *g_state = NULL;
static pthread_mutex_t  g_init_mutex = PTHREAD_MUTEX_INITIALIZER;

// Forward declarations
static void *flush_thread_fn(void *arg);
static bool  flush_buffer_locked(void);
static bool  try_reconnect_locked(void);
static void  cleanup_connection(void);

// --------------------------------------------------------------------------
// Error handling
// --------------------------------------------------------------------------

static void log_and_handle_error(line_sender_error *err, const char *op) {
    if (!err) return;

    size_t len;
    const char *msg = line_sender_error_msg(err, &len);
    DLOG_ERROR("QuestDB %s: %.*s", op, (int)len, msg);
    line_sender_error_free(err);

    if (g_state) {
        atomic_store(&g_state->healthy, false);
        g_state->failed++;
        g_state->consecutive_failures++;
    }
}

static void log_error_no_state(line_sender_error *err, const char *op) {
    if (!err) return;
    size_t len;
    const char *msg = line_sender_error_msg(err, &len);
    DLOG_WARN("QuestDB %s: %.*s", op, (int)len, msg);
    line_sender_error_free(err);
}

// --------------------------------------------------------------------------
// Connection management
// --------------------------------------------------------------------------

static bool create_connection(void) {
    char conn_str[CONN_STR_SIZE];
    snprintf(conn_str, sizeof(conn_str), "tcp::addr=%s:%u;",
             g_state->config.host, g_state->config.ilp_port);

    line_sender_error *err = NULL;
    g_state->sender = line_sender_from_conf(QDB_UTF8(conn_str), &err);
    if (!g_state->sender) {
        log_error_no_state(err, "connect");
        return false;
    }

    g_state->buffer = line_sender_buffer_new_for_sender(g_state->sender);
    if (!g_state->buffer) {
        DLOG_ERROR("QuestDB: failed to create buffer");
        line_sender_close(g_state->sender);
        g_state->sender = NULL;
        return false;
    }

    atomic_store(&g_state->healthy, true);
    g_state->consecutive_failures = 0;
    g_state->reconnect_attempts = 0;
    return true;
}

static void cleanup_connection(void) {
    if (!g_state) return;

    if (g_state->buffer) {
        line_sender_buffer_free(g_state->buffer);
        g_state->buffer = NULL;
    }
    if (g_state->sender) {
        line_sender_close(g_state->sender);
        g_state->sender = NULL;
    }
}

static bool try_reconnect_locked(void) {
    uint64_t now = current_time_millis();

    // Exponential backoff: 1s, 2s, 4s, 8s... capped at 60s
    // Cap attempts to prevent shift overflow (2^6 * 1000 = 64000 > 60000)
    uint32_t capped_attempts = g_state->reconnect_attempts > 6 ? 6 : g_state->reconnect_attempts;
    uint64_t backoff = (1ULL << capped_attempts) * 1000;
    if (backoff > MAX_BACKOFF_MS) backoff = MAX_BACKOFF_MS;

    if (now - g_state->last_reconnect_ms < backoff) {
        return false;  // Too soon
    }

    g_state->last_reconnect_ms = now;
    g_state->reconnect_attempts++;

    DLOG_INFO("QuestDB reconnecting (attempt %u, backoff %llums)",
              g_state->reconnect_attempts, (unsigned long long)backoff);

    cleanup_connection();

    if (create_connection()) {
        DLOG_INFO("QuestDB reconnected to %s:%u",
                  g_state->config.host, g_state->config.ilp_port);
        return true;
    }

    DLOG_WARN("QuestDB reconnection failed to %s:%u",
              g_state->config.host, g_state->config.ilp_port);
    return false;
}

// --------------------------------------------------------------------------
// Configuration validation
// --------------------------------------------------------------------------

static bool validate_config(const T_DATUM_QUESTDB_CONFIG *cfg) {
    if (!cfg->host[0]) {
        DLOG_ERROR("QuestDB: empty hostname");
        return false;
    }
    if (cfg->ilp_port == 0) {
        DLOG_ERROR("QuestDB: invalid port %u", cfg->ilp_port);
        return false;
    }
    if (cfg->batch_size < 10 || cfg->batch_size > 10000) {
        DLOG_ERROR("QuestDB: batch_size %u out of range [10-10000]", cfg->batch_size);
        return false;
    }
    if (cfg->flush_interval_ms < 1000 || cfg->flush_interval_ms > 60000) {
        DLOG_ERROR("QuestDB: flush_interval_ms %u out of range [1000-60000]", cfg->flush_interval_ms);
        return false;
    }
    return true;
}

// --------------------------------------------------------------------------
// Initialization and shutdown
// --------------------------------------------------------------------------

bool datum_questdb_init(const T_DATUM_QUESTDB_CONFIG *config) {
    if (!config) {
        DLOG_ERROR("QuestDB: NULL config");
        return false;
    }

    if (!config->enabled) {
        DLOG_INFO("QuestDB: disabled");
        return true;
    }

    if (!validate_config(config)) {
        return false;
    }

    pthread_mutex_lock(&g_init_mutex);

    if (g_state) {
        DLOG_WARN("QuestDB: already initialized");
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }

    g_state = calloc(1, sizeof(*g_state));
    if (!g_state) {
        DLOG_ERROR("QuestDB: allocation failed");
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }

    g_state->config = *config;

    if (pthread_mutex_init(&g_state->mutex, NULL) != 0) {
        DLOG_ERROR("QuestDB: mutex init failed");
        free(g_state);
        g_state = NULL;
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }

    DLOG_INFO("QuestDB: connecting to %s:%u", config->host, config->ilp_port);

    // Retry connection at startup to handle service ordering
    bool connected = false;
    for (int i = 0; i < STARTUP_RETRIES && !connected; i++) {
        if (i > 0) {
            DLOG_INFO("QuestDB: retry %d/%d in %ds", i + 1, STARTUP_RETRIES, STARTUP_RETRY_DELAY_SEC);
            sleep(STARTUP_RETRY_DELAY_SEC);
        }
        connected = create_connection();
        if (!connected) {
            DLOG_WARN("QuestDB: attempt %d/%d failed", i + 1, STARTUP_RETRIES);
        }
    }

    if (!connected) {
        DLOG_WARN("QuestDB: initial connection failed, will retry in background");
        atomic_store(&g_state->healthy, false);
        g_state->last_reconnect_ms = current_time_millis();
    } else {
        DLOG_INFO("QuestDB: connected to %s:%u", config->host, config->ilp_port);
    }

    atomic_store(&g_state->running, true);

    if (pthread_create(&g_state->flush_thread, NULL, flush_thread_fn, NULL) != 0) {
        DLOG_ERROR("QuestDB: thread creation failed");
        cleanup_connection();
        pthread_mutex_destroy(&g_state->mutex);
        free(g_state);
        g_state = NULL;
        pthread_mutex_unlock(&g_init_mutex);
        return false;
    }

    pthread_mutex_unlock(&g_init_mutex);

    DLOG_INFO("QuestDB: initialized (flush_interval=%ums, batch_size=%u)",
              config->flush_interval_ms, config->batch_size);
    return true;
}

void datum_questdb_shutdown(void) {
    if (!g_state) return;

    DLOG_INFO("QuestDB: shutting down");

    atomic_store(&g_state->running, false);
    pthread_join(g_state->flush_thread, NULL);

    pthread_mutex_lock(&g_state->mutex);
    flush_buffer_locked();
    pthread_mutex_unlock(&g_state->mutex);

    DLOG_INFO("QuestDB: sent=%llu failed=%llu",
              (unsigned long long)g_state->sent,
              (unsigned long long)g_state->failed);

    cleanup_connection();
    pthread_mutex_destroy(&g_state->mutex);
    free(g_state);
    g_state = NULL;

    DLOG_INFO("QuestDB: shutdown complete");
}

// --------------------------------------------------------------------------
// Flush operations
// --------------------------------------------------------------------------

static bool flush_buffer_locked(void) {
    if (!g_state || g_state->pending == 0) {
        return true;
    }

    // Try reconnect if unhealthy
    if (!atomic_load(&g_state->healthy) &&
        g_state->consecutive_failures >= RECONNECT_THRESHOLD) {
        if (!try_reconnect_locked()) {
            return false;
        }
    }

    if (!g_state->sender) {
        return false;
    }

    line_sender_error *err = NULL;
    if (!line_sender_flush(g_state->sender, g_state->buffer, &err)) {
        log_and_handle_error(err, "flush");
        if (g_state->consecutive_failures >= RECONNECT_THRESHOLD) {
            try_reconnect_locked();
        }
        return false;
    }

    g_state->sent += g_state->pending;
    g_state->pending = 0;
    g_state->flushes++;
    g_state->consecutive_failures = 0;
    atomic_store(&g_state->healthy, true);

    return true;
}

void datum_questdb_flush(void) {
    if (!g_state) return;

    pthread_mutex_lock(&g_state->mutex);
    flush_buffer_locked();
    pthread_mutex_unlock(&g_state->mutex);
}

static void *flush_thread_fn(void *arg) {
    (void)arg;
    DLOG_DEBUG("QuestDB: flush thread started");

    while (atomic_load(&g_state->running)) {
        usleep((useconds_t)g_state->config.flush_interval_ms * 1000U);

        if (!atomic_load(&g_state->running)) break;

        pthread_mutex_lock(&g_state->mutex);

        // Reconnect if unhealthy
        if (!atomic_load(&g_state->healthy)) {
            try_reconnect_locked();
        }

        // Flush if healthy
        if (atomic_load(&g_state->healthy)) {
            flush_buffer_locked();
        }

        pthread_mutex_unlock(&g_state->mutex);
    }

    DLOG_DEBUG("QuestDB: flush thread exiting");
    return NULL;
}

// --------------------------------------------------------------------------
// Metric recording
// --------------------------------------------------------------------------

void datum_questdb_record_hashrate(const T_DATUM_HASHRATE_METRIC *m) {
    if (!g_state || !m || !atomic_load(&g_state->healthy)) return;

    pthread_mutex_lock(&g_state->mutex);

    if (!g_state->buffer) goto cleanup;

    line_sender_error *err = NULL;

    // Table and symbols
    QDB_CHECK(line_sender_buffer_table(g_state->buffer, QDB_TABLE(TBL_HASHRATE), &err), "table");
    QDB_CHECK(line_sender_buffer_symbol(g_state->buffer, QDB_COL("worker"), QDB_UTF8(m->worker_id), &err), "worker");
    QDB_CHECK(line_sender_buffer_symbol(g_state->buffer, QDB_COL("ip"), QDB_UTF8(m->machine_ip), &err), "ip");
    QDB_CHECK(line_sender_buffer_symbol(g_state->buffer, QDB_COL("user_agent"), QDB_UTF8(m->user_agent), &err), "user_agent");

    // Numeric columns
    QDB_CHECK(line_sender_buffer_column_f64(g_state->buffer, QDB_COL("calculated_th"), m->calculated_hashrate_th, &err), "calculated_th");
    QDB_CHECK(line_sender_buffer_column_i64(g_state->buffer, QDB_COL("shares_accepted"), (int64_t)m->shares_accepted, &err), "shares_accepted");
    QDB_CHECK(line_sender_buffer_column_i64(g_state->buffer, QDB_COL("shares_rejected"), (int64_t)m->shares_rejected, &err), "shares_rejected");
    QDB_CHECK(line_sender_buffer_column_i64(g_state->buffer, QDB_COL("diff_accepted"), (int64_t)m->difficulty_accepted, &err), "diff_accepted");
    QDB_CHECK(line_sender_buffer_column_i64(g_state->buffer, QDB_COL("diff_rejected"), (int64_t)m->difficulty_rejected, &err), "diff_rejected");
    QDB_CHECK(line_sender_buffer_column_f64(g_state->buffer, QDB_COL("shares_per_min"), m->shares_per_min, &err), "shares_per_min");
    QDB_CHECK(line_sender_buffer_column_i64(g_state->buffer, QDB_COL("uptime_seconds"), (int64_t)m->uptime_seconds, &err), "uptime_seconds");
    QDB_CHECK(line_sender_buffer_column_i64(g_state->buffer, QDB_COL("last_share_delay_ms"), (int64_t)m->last_share_delay_ms, &err), "last_share_delay_ms");
    QDB_CHECK(line_sender_buffer_column_i64(g_state->buffer, QDB_COL("current_diff"), (int64_t)m->current_diff, &err), "current_diff");
    QDB_CHECK(line_sender_buffer_column_i64(g_state->buffer, QDB_COL("coinbase_type"), (int64_t)m->coinbase_type, &err), "coinbase_type");
    QDB_CHECK(line_sender_buffer_column_i64(g_state->buffer, QDB_COL("connection_duration"), (int64_t)m->connection_duration, &err), "connection_duration");

    // Timestamp (cast to int64_t as QuestDB API uses signed nanoseconds)
    QDB_CHECK(line_sender_buffer_at_nanos(g_state->buffer, (int64_t)m->timestamp_ns, &err), "timestamp");

    g_state->pending++;

    if (g_state->pending >= g_state->config.batch_size) {
        flush_buffer_locked();
    }

cleanup:
    pthread_mutex_unlock(&g_state->mutex);
}

void datum_questdb_record_share(const char *worker_id, const char *machine_ip,
                                 uint64_t difficulty, bool accepted,
                                 const char *reject_reason) {
    if (!g_state || !worker_id || !atomic_load(&g_state->healthy)) return;

    pthread_mutex_lock(&g_state->mutex);

    if (!g_state->buffer) goto cleanup;

    line_sender_error *err = NULL;
    const char *status = accepted ? "accepted" : "rejected";

    QDB_CHECK(line_sender_buffer_table(g_state->buffer, QDB_TABLE(TBL_SHARES), &err), "table");
    QDB_CHECK(line_sender_buffer_symbol(g_state->buffer, QDB_COL("worker"), QDB_UTF8(worker_id), &err), "worker");
    QDB_CHECK(line_sender_buffer_symbol(g_state->buffer, QDB_COL("ip"), QDB_UTF8_OR(machine_ip, "unknown"), &err), "ip");
    QDB_CHECK(line_sender_buffer_symbol(g_state->buffer, QDB_COL("status"), QDB_UTF8(status), &err), "status");

    if (!accepted && reject_reason) {
        QDB_CHECK(line_sender_buffer_symbol(g_state->buffer, QDB_COL("reason"), QDB_UTF8(reject_reason), &err), "reason");
    }

    QDB_CHECK(line_sender_buffer_column_i64(g_state->buffer, QDB_COL("difficulty"), (int64_t)difficulty, &err), "difficulty");
    QDB_CHECK(line_sender_buffer_at_nanos(g_state->buffer, line_sender_now_nanos(), &err), "timestamp");

    g_state->pending++;
    flush_buffer_locked();  // Immediate flush for shares

cleanup:
    pthread_mutex_unlock(&g_state->mutex);
}

// --------------------------------------------------------------------------
// Status and statistics
// --------------------------------------------------------------------------

bool datum_questdb_is_healthy(void) {
    return g_state && atomic_load(&g_state->healthy);
}

bool datum_questdb_is_initialized(void) {
    return g_state != NULL;
}

void datum_questdb_get_stats(uint64_t *sent, uint64_t *failed, uint64_t *pending) {
    if (!g_state) {
        if (sent) *sent = 0;
        if (failed) *failed = 0;
        if (pending) *pending = 0;
        return;
    }

    pthread_mutex_lock(&g_state->mutex);
    if (sent) *sent = g_state->sent;
    if (failed) *failed = g_state->failed;
    if (pending) *pending = g_state->pending;
    pthread_mutex_unlock(&g_state->mutex);
}

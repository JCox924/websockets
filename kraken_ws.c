#include "cb_ws.h"

#include <libwebsockets.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <inttypes.h>
#include <errno.h>

#include "jsmn.h"

#define KRAKEN_PRODUCT "ETH/USD"
#define KRAKEN_DEFAULT_ENDPOINT "wss://ws.kraken.com"

static volatile sig_atomic_t g_should_exit = 0;

static void on_sigint(int sig) {
    (void)sig;
    g_should_exit = 1;
}

/* ---------- Time helpers ---------- */

static uint64_t now_realtime_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static uint64_t now_monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void iso8601_from_ns(uint64_t ns, char *out, size_t out_sz) {
    if (!out || out_sz == 0) return;

    time_t sec = (time_t)(ns / 1000000000ULL);
    struct tm tm;
    gmtime_r(&sec, &tm);

    snprintf(out, out_sz, "%04d-%02d-%02dT%02d:%02d:%02dZ",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec);
}

/* ---------- JSON helpers (jsmn) ---------- */

static int json_token_streq(const char *json, const jsmntok_t *tok, const char *s) {
    size_t len = (size_t)(tok->end - tok->start);
    return (tok->type == JSMN_STRING &&
            strlen(s) == len &&
            strncmp(json + tok->start, s, len) == 0);
}

static int skip_token(jsmntok_t *toks, int idx) {
    int next = idx + 1;
    if (toks[idx].type == JSMN_ARRAY || toks[idx].type == JSMN_OBJECT) {
        for (int i = 0; i < toks[idx].size; i++) {
            next = skip_token(toks, next);
        }
    }
    return next;
}

static int token_count(jsmntok_t *toks, int ntok, int idx) {
    int next = skip_token(toks, idx);
    if (next > ntok) return 0;
    return next - idx;
}

static int json_get_string(const char *json, jsmntok_t *toks, int ntok,
                           const char *key, char *out, size_t out_sz) {
    for (int i = 1; i < ntok - 1; i++) {
        if (toks[i].type == JSMN_STRING && json_token_streq(json, &toks[i], key)) {
            jsmntok_t *v = &toks[i + 1];
            if (v->type != JSMN_STRING) return -1;
            size_t len = (size_t)(v->end - v->start);
            if (len >= out_sz) len = out_sz - 1;
            memcpy(out, json + v->start, len);
            out[len] = '\0';
            return 0;
        }
    }
    return -1;
}

static int json_get_number(const char *json, jsmntok_t *toks, int ntok,
                           const char *key, double *out) {
    for (int i = 1; i < ntok - 1; i++) {
        if (toks[i].type == JSMN_STRING && json_token_streq(json, &toks[i], key)) {
            jsmntok_t *v = &toks[i + 1];
            if (v->type != JSMN_STRING && v->type != JSMN_PRIMITIVE) return -1;
            char tmp[64];
            size_t len = (size_t)(v->end - v->start);
            if (len >= sizeof(tmp)) len = sizeof(tmp) - 1;
            memcpy(tmp, json + v->start, len);
            tmp[len] = '\0';
            *out = strtod(tmp, NULL);
            return 0;
        }
    }
    return -1;
}

static int json_get_array_element_number(const char *json, jsmntok_t *toks, int ntok,
                                         const char *key, int array_idx, double *out) {
    for (int i = 1; i < ntok - 1; i++) {
        if (toks[i].type == JSMN_STRING && json_token_streq(json, &toks[i], key)) {
            jsmntok_t *arr = &toks[i + 1];
            if (arr->type != JSMN_ARRAY || array_idx >= arr->size) return -1;
            int j = i + 2; /* first element */
            for (int k = 0; k < arr->size; k++) {
                if (k == array_idx) {
                    jsmntok_t *elem = &toks[j];
                    char tmp[64];
                    size_t len = (size_t)(elem->end - elem->start);
                    if (len >= sizeof(tmp)) len = sizeof(tmp) - 1;
                    memcpy(tmp, json + elem->start, len);
                    tmp[len] = '\0';
                    *out = strtod(tmp, NULL);
                    return 0;
                }
                j = skip_token(toks, j);
            }
            return -1;
        }
    }
    return -1;
}

/* ---------- App state + buffering ---------- */

typedef struct app_state {
    eth_stats_t *stats;

    unsigned char *rx_buf;
    size_t rx_len;
    size_t rx_cap;

    int subscribed;

    /* CSV logging */
    FILE *csv;
    int csv_header_written;
    double last_volume_24h;
} app_state_t;

static void ensure_rx_capacity(app_state_t *st, size_t needed) {
    if (needed <= st->rx_cap) return;
    size_t new_cap = st->rx_cap ? st->rx_cap : 4096;
    while (new_cap < needed) new_cap *= 2;
    unsigned char *p = (unsigned char*)realloc(st->rx_buf, new_cap);
    if (!p) return;
    st->rx_buf = p;
    st->rx_cap = new_cap;
}

static void reset_rx(app_state_t *st) {
    st->rx_len = 0;
}

/* ---------- CSV helpers ---------- */

static int csv_open(app_state_t *st, const char *path) {
    if (!path || !*path) return 0;

    st->csv = fopen(path, "a+");
    if (!st->csv) {
        fprintf(stderr, "CSV open failed (%s): %s\n", path, strerror(errno));
        return -1;
    }

    setvbuf(st->csv, NULL, _IOFBF, 1 << 20);

    fseek(st->csv, 0, SEEK_END);
    long sz = ftell(st->csv);
    if (sz > 0) {
        st->csv_header_written = 1;
    }

    fprintf(stderr, "CSV logging to: %s\n", path);
    return 0;
}

static void csv_close(app_state_t *st) {
    if (st->csv) {
        fflush(st->csv);
        fclose(st->csv);
        st->csv = NULL;
    }
}

static void csv_write_header_if_needed(app_state_t *st) {
    if (!st->csv || st->csv_header_written) return;

    fprintf(st->csv,
            "recv_rt_ns,recv_mono_ns,server_time_iso,server_time_ns,"
            "latency_ms,product_id,sequence,"
            "price,best_bid,best_ask,best_bid_size,best_ask_size,"
            "spread,mid,imbalance,"
            "volume_24h,trade_size_est\n");
    st->csv_header_written = 1;
    fflush(st->csv);
}

static void csv_write_ticker_row(app_state_t *st,
                                 const eth_stats_t *s,
                                 uint64_t recv_rt_ns,
                                 uint64_t recv_mono_ns,
                                 uint64_t server_ns,
                                 double spread,
                                 double mid,
                                 double imbalance,
                                 double trade_size_est) {
    if (!st->csv) return;
    csv_write_header_if_needed(st);

    fprintf(st->csv,
            "%" PRIu64 ",%" PRIu64 ",%s,%" PRIu64 ",%.6f,%s,%" PRIu64 ","
            "%.8f,%.8f,%.8f,%.8f,%.8f,"
            "%.8f,%.8f,%.8f,"
            "%.8f,%.8f\n",
            recv_rt_ns,
            recv_mono_ns,
            s->time_iso[0] ? s->time_iso : "",
            server_ns,
            s->last_ticker_latency_ms,
            s->product_id,
            s->last_seq,
            s->price,
            s->best_bid,
            s->best_ask,
            s->best_bid_size,
            s->best_ask_size,
            spread,
            mid,
            imbalance,
            s->volume_24h,
            trade_size_est);
    static uint64_t row_count = 0;
    row_count++;
    if ((row_count % 200) == 0) {
        fflush(st->csv);
    }
}

/* ---------- Kraken message parsing ---------- */

static void handle_ticker(app_state_t *st, const char *msg, jsmntok_t *toks, int ntok,
                          int data_idx, int type_idx, int pair_idx,
                          uint64_t recv_rt_ns, uint64_t recv_mono_ns) {
    (void)type_idx;
    (void)pair_idx;

    eth_stats_t *s = st->stats;

    int data_ntok = token_count(toks, ntok, data_idx);
    if (data_ntok <= 0) return;

    json_get_array_element_number(msg, &toks[data_idx], data_ntok, "c", 0, &s->price);
    json_get_array_element_number(msg, &toks[data_idx], data_ntok, "b", 0, &s->best_bid);
    json_get_array_element_number(msg, &toks[data_idx], data_ntok, "a", 0, &s->best_ask);
    json_get_array_element_number(msg, &toks[data_idx], data_ntok, "b", 1, &s->best_bid_size);
    json_get_array_element_number(msg, &toks[data_idx], data_ntok, "a", 1, &s->best_ask_size);
    json_get_array_element_number(msg, &toks[data_idx], data_ntok, "v", 1, &s->volume_24h);

    /* Kraken ticker messages do not include server time; stamp with recv time. */
    iso8601_from_ns(recv_rt_ns, s->time_iso, sizeof(s->time_iso));
    s->last_seq = 0;
    s->last_ticker_latency_ms = 0.0;
    strncpy(s->product_id, KRAKEN_PRODUCT, sizeof(s->product_id) - 1);

    s->updates += 1;

    double spread = 0.0;
    double mid = 0.0;
    double imbalance = 0.0;

    if (s->best_ask > 0.0 && s->best_bid > 0.0 && s->best_ask >= s->best_bid) {
        spread = s->best_ask - s->best_bid;
        mid = (s->best_ask + s->best_bid) / 2.0;
    }

    double denom = (s->best_bid_size + s->best_ask_size);
    if (denom > 0.0) {
        imbalance = (s->best_bid_size - s->best_ask_size) / denom;
    }

    double trade_size_est = 0.0;
    if (st->last_volume_24h > 0.0 && s->volume_24h >= st->last_volume_24h) {
        trade_size_est = s->volume_24h - st->last_volume_24h;
    }
    st->last_volume_24h = s->volume_24h;

    csv_write_ticker_row(st, s, recv_rt_ns, recv_mono_ns, recv_rt_ns,
                         spread, mid, imbalance, trade_size_est);

    printf("[KRAKEN] price=%.2f bid=%.2f ask=%.2f vol24h=%.4f time=%s\n",
           s->price, s->best_bid, s->best_ask, s->volume_24h,
           s->time_iso[0] ? s->time_iso : "(n/a)");
    fflush(stdout);
}

static void parse_and_update(app_state_t *st, const char *msg, size_t msg_len,
                             uint64_t recv_rt_ns, uint64_t recv_mono_ns) {
    jsmn_parser p;
    jsmntok_t toks[256];

    jsmn_init(&p);
    int ntok = jsmn_parse(&p, msg, (int)msg_len, toks, (int)(sizeof(toks) / sizeof(toks[0])));
    if (ntok < 1) return;

    if (toks[0].type == JSMN_OBJECT) {
        char event[32] = {0};
        if (json_get_string(msg, toks, ntok, "event", event, sizeof(event)) == 0) {
            if (strcmp(event, "heartbeat") == 0) {
                st->stats->heartbeats += 1;
                st->stats->last_hb_latency_ms = 0.0;
                st->stats->last_hb_interarrival_ms = 0.0;
                printf("[KRAKEN] HEARTBEAT count=%" PRIu64 "\n", st->stats->heartbeats);
                fflush(stdout);
            } else if (strcmp(event, "subscriptionStatus") == 0) {
                char status[32] = {0};
                if (json_get_string(msg, toks, ntok, "status", status, sizeof(status)) == 0) {
                    fprintf(stderr, "Subscription status: %s\n", status);
                }
            }
        }
        return;
    }

    if (toks[0].type != JSMN_ARRAY || toks[0].size < 4) return;

    int idx = 1; /* channel id */
    idx = skip_token(toks, idx); /* data object */
    int data_idx = idx;
    idx = skip_token(toks, idx); /* type string */
    int type_idx = idx;
    idx = skip_token(toks, idx); /* product pair */
    int pair_idx = idx;

    if (pair_idx >= ntok || type_idx >= ntok || data_idx >= ntok) return;
    if (toks[type_idx].type != JSMN_STRING || !json_token_streq(msg, &toks[type_idx], "ticker")) return;

    handle_ticker(st, msg, toks, ntok, data_idx, type_idx, pair_idx, recv_rt_ns, recv_mono_ns);
}

/* ---------- libwebsockets callback ---------- */

static int ws_callback(struct lws *wsi, enum lws_callback_reasons reason,
                       void *user, void *in, size_t len) {
    (void)user;
    app_state_t *st = (app_state_t *)lws_context_user(lws_get_context(wsi));

    switch (reason) {
        case LWS_CALLBACK_CLIENT_ESTABLISHED: {
            const char *sub =
                "{"
                  "\"event\":\"subscribe\"," 
                  "\"pair\":[\"" KRAKEN_PRODUCT "\"],"
                  "\"subscription\":{\"name\":\"ticker\"}"
                "}";

            unsigned char buf[LWS_PRE + 256];
            size_t sub_len = strlen(sub);
            if (LWS_PRE + sub_len >= sizeof(buf)) return -1;

            memcpy(&buf[LWS_PRE], sub, sub_len);
            int n = lws_write(wsi, &buf[LWS_PRE], sub_len, LWS_WRITE_TEXT);
            if (n < 0) return -1;

            st->subscribed = 1;
            reset_rx(st);

            fprintf(stderr, "Connected; subscribed to Kraken %s ticker.\n", KRAKEN_PRODUCT);
            break;
        }

        case LWS_CALLBACK_CLIENT_RECEIVE: {
            size_t remaining = lws_remaining_packet_payload(wsi);
            int is_final = lws_is_final_fragment(wsi);

            ensure_rx_capacity(st, st->rx_len + len + 1);
            if (!st->rx_buf) return -1;

            memcpy(st->rx_buf + st->rx_len, in, len);
            st->rx_len += len;
            st->rx_buf[st->rx_len] = '\0';

            if (is_final && remaining == 0) {
                uint64_t recv_rt_ns = now_realtime_ns();
                uint64_t recv_mono_ns = now_monotonic_ns();

                parse_and_update(st, (const char *)st->rx_buf, st->rx_len, recv_rt_ns, recv_mono_ns);
                reset_rx(st);
            }
            break;
        }

        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
            fprintf(stderr, "Connection error: %s\n", in ? (const char *)in : "(unknown)");
            g_should_exit = 1;
            break;

        case LWS_CALLBACK_CLIENT_CLOSED:
            fprintf(stderr, "Connection closed.\n");
            g_should_exit = 1;
            break;

        default:
            break;
    }

    return 0;
}

/* ---------- Public entrypoint ---------- */

int kraken_ws_run_eth_ticker(const char *endpoint_wss, const char *csv_path, eth_stats_t *out_stats) {
    if (!endpoint_wss || !*endpoint_wss) endpoint_wss = KRAKEN_DEFAULT_ENDPOINT;
    if (!out_stats) return 2;

    memset(out_stats, 0, sizeof(*out_stats));
    strncpy(out_stats->product_id, KRAKEN_PRODUCT, sizeof(out_stats->product_id) - 1);

    signal(SIGINT, on_sigint);
    signal(SIGTERM, on_sigint);

    app_state_t st;
    memset(&st, 0, sizeof(st));
    st.stats = out_stats;

    if (csv_open(&st, csv_path) != 0) {
        return 5;
    }

    struct lws_protocols protocols[] = {
        { "kraken-proto", ws_callback, 0, 8192, 0, NULL, 0 },
        { NULL, NULL, 0, 0, 0, NULL, 0 }
    };

    struct lws_context_creation_info info;
    memset(&info, 0, sizeof(info));
    info.port = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols;
    info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    info.user = &st;

    struct lws_context *ctx = lws_create_context(&info);
    if (!ctx) {
        fprintf(stderr, "Failed to create lws context\n");
        csv_close(&st);
        return 3;
    }

    /* Parse endpoint URL (expects wss://host[:port]) */
    const char *p = strstr(endpoint_wss, "://");
    const char *hostport = p ? p + 3 : endpoint_wss;

    char host[256] = {0};
    const char *slash = strchr(hostport, '/');
    size_t hostport_len = slash ? (size_t)(slash - hostport) : strlen(hostport);
    if (hostport_len >= sizeof(host)) hostport_len = sizeof(host) - 1;
    memcpy(host, hostport, hostport_len);
    host[hostport_len] = '\0';

    int port = 443;
    char *colon = strrchr(host, ':');
    if (colon) {
        *colon = '\0';
        port = atoi(colon + 1);
        if (port <= 0) port = 443;
    }

    struct lws_client_connect_info ccinfo;
    memset(&ccinfo, 0, sizeof(ccinfo));
    ccinfo.context = ctx;
    ccinfo.address = host;
    ccinfo.port = port;
    ccinfo.path = "/";
    ccinfo.host = host;
    ccinfo.origin = host;
    ccinfo.protocol = protocols[0].name;
    ccinfo.ssl_connection = LCCSCF_USE_SSL;

    struct lws *wsi = lws_client_connect_via_info(&ccinfo);
    if (!wsi) {
        fprintf(stderr, "Failed to connect to %s\n", endpoint_wss);
        lws_context_destroy(ctx);
        csv_close(&st);
        return 4;
    }

    fprintf(stderr, "Connecting to Kraken %s ... (Ctrl+C to stop)\n", endpoint_wss);

    while (!g_should_exit) {
        lws_service(ctx, 200);
    }

    free(st.rx_buf);
    lws_context_destroy(ctx);
    csv_close(&st);
    return 0;
}


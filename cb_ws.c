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

#define PRODUCT_ID "ETH-USD"
#define DEFAULT_ENDPOINT "wss://ws-feed.exchange.coinbase.com"

static volatile sig_atomic_t g_default_stop_flag = 0;
static volatile sig_atomic_t *g_stop_flag = &g_default_stop_flag;

static void request_stop(void) {
    if (g_stop_flag) {
        *g_stop_flag = 1;
    }
}

static void on_sigint(int sig) {
    (void)sig;
    request_stop();
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

/* Parse RFC3339 UTC timestamps like:
   2025-12-16T06:16:56.509354Z -> epoch ns (UTC)
   Returns 0 on failure. */
static uint64_t parse_rfc3339_utc_ns(const char *s) {
    if (!s || !*s) return 0;

    struct tm tm;
    memset(&tm, 0, sizeof(tm));

    /* Copy until '.' or 'Z' */
    char base[32] = {0};
    size_t i = 0;
    for (; s[i] && s[i] != '.' && s[i] != 'Z' && i < sizeof(base) - 1; i++) {
        base[i] = s[i];
    }
    base[i] = '\0';

    char *r = strptime(base, "%Y-%m-%dT%H:%M:%S", &tm);
    if (!r) return 0;

    /* Fractional seconds */
    uint64_t frac_ns = 0;
    const char *p = s + i;
    if (*p == '.') {
        p++;
        uint64_t scale = 100000000ULL; /* first digit => 1e8 ns */
        while (*p >= '0' && *p <= '9' && scale > 0) {
            frac_ns += (uint64_t)(*p - '0') * scale;
            scale /= 10;
            p++;
        }
        while (*p >= '0' && *p <= '9') p++; /* skip extra digits */
    }

    while (*p == ' ') p++;
    if (*p != 'Z') return 0;

    /* Convert tm (UTC) -> epoch seconds */
    time_t sec = timegm(&tm);
    if (sec < 0) return 0;

    return (uint64_t)sec * 1000000000ULL + frac_ns;
}

/* ---------- JSON helpers (jsmn) ---------- */

static int json_token_streq(const char *json, const jsmntok_t *tok, const char *s) {
    size_t len = (size_t)(tok->end - tok->start);
    return (tok->type == JSMN_STRING &&
            strlen(s) == len &&
            strncmp(json + tok->start, s, len) == 0);
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

static int json_get_u64(const char *json, jsmntok_t *toks, int ntok,
                        const char *key, uint64_t *out) {
    double d = 0.0;
    if (json_get_number(json, toks, ntok, key, &d) == 0) {
        if (d < 0) d = 0;
        *out = (uint64_t)d;
        return 0;
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

    /* Heartbeat cadence */
    uint64_t last_hb_mono_ns;

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

    st->csv = fopen(path, "a+");   /* read/write, append */
    if (!st->csv) {
        fprintf(stderr, "CSV open failed (%s): %s\n", path, strerror(errno));
        return -1;
    }

    /* Optional: keep buffering for performance */
    setvbuf(st->csv, NULL, _IOFBF, 1 << 20);

    /* Detect whether file already has content */
    fseek(st->csv, 0, SEEK_END);
    long sz = ftell(st->csv);
    if (sz > 0) {
        st->csv_header_written = 1; /* don't write another header */
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
    if ((row_count % 200) == 0) {   /* flush every 200 ticks */
        fflush(st->csv);
    }

}

/* ---------- Message parsing ---------- */

static void parse_and_update(app_state_t *st, const char *msg, size_t msg_len,
                             uint64_t recv_rt_ns, uint64_t recv_mono_ns) {
    jsmn_parser p;
    jsmntok_t toks[256];

    jsmn_init(&p);
    int ntok = jsmn_parse(&p, msg, (int)msg_len, toks, (int)(sizeof(toks) / sizeof(toks[0])));
    if (ntok < 1 || toks[0].type != JSMN_OBJECT) return;

    char type[32] = {0};
    if (json_get_string(msg, toks, ntok, "type", type, sizeof(type)) != 0) return;

    /* ---- Heartbeat ---- */
    if (strcmp(type, "heartbeat") == 0) {
        char product[16] = {0};
        if (json_get_string(msg, toks, ntok, "product_id", product, sizeof(product)) != 0) return;
        if (strcmp(product, PRODUCT_ID) != 0) return;

        eth_stats_t *s = st->stats;
        s->heartbeats += 1;

        if (st->last_hb_mono_ns != 0 && recv_mono_ns > st->last_hb_mono_ns) {
            s->last_hb_interarrival_ms = (double)(recv_mono_ns - st->last_hb_mono_ns) / 1e6;
        }
        st->last_hb_mono_ns = recv_mono_ns;

        char hb_time[40] = {0};
        if (json_get_string(msg, toks, ntok, "time", hb_time, sizeof(hb_time)) == 0) {
            uint64_t hb_ns = parse_rfc3339_utc_ns(hb_time);
            if (hb_ns > 0 && recv_rt_ns >= hb_ns) {
                s->last_hb_latency_ms = (double)(recv_rt_ns - hb_ns) / 1e6;
            }
        }

        /* Optional: comment out if too noisy */
        printf("[%-6s] HEARTBEAT inter=%.2fms hb_lat=%.2fms count=%" PRIu64 "\n",
               PRODUCT_ID, s->last_hb_interarrival_ms, s->last_hb_latency_ms, s->heartbeats);
        fflush(stdout);
        return;
    }

    /* ---- Ticker ---- */
    if (strcmp(type, "ticker") != 0) return;

    char product[16] = {0};
    if (json_get_string(msg, toks, ntok, "product_id", product, sizeof(product)) != 0) return;
    if (strcmp(product, PRODUCT_ID) != 0) return;

    eth_stats_t *s = st->stats;
    strncpy(s->product_id, product, sizeof(s->product_id) - 1);

    (void)json_get_number(msg, toks, ntok, "price", &s->price);
    (void)json_get_number(msg, toks, ntok, "best_bid", &s->best_bid);
    (void)json_get_number(msg, toks, ntok, "best_ask", &s->best_ask);
    (void)json_get_number(msg, toks, ntok, "best_bid_size", &s->best_bid_size);
    (void)json_get_number(msg, toks, ntok, "best_ask_size", &s->best_ask_size);
    (void)json_get_number(msg, toks, ntok, "volume_24h", &s->volume_24h);
    (void)json_get_string(msg, toks, ntok, "time", s->time_iso, sizeof(s->time_iso));
    (void)json_get_u64(msg, toks, ntok, "sequence", &s->last_seq);

    /* 1.A: ticker event latency (recv realtime - server time) */
    uint64_t server_ns = parse_rfc3339_utc_ns(s->time_iso);
    if (server_ns > 0 && recv_rt_ns >= server_ns) {
        s->last_ticker_latency_ms = (double)(recv_rt_ns - server_ns) / 1e6;
    }

    s->updates += 1;

    /* Derived features */
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

    /* Estimate trade size from volume_24h delta (best effort) */
    double trade_size_est = 0.0;
    if (st->last_volume_24h > 0.0 && s->volume_24h >= st->last_volume_24h) {
        trade_size_est = s->volume_24h - st->last_volume_24h;
    }
    st->last_volume_24h = s->volume_24h;

    /* CSV row */
    csv_write_ticker_row(st, s, recv_rt_ns, recv_mono_ns, server_ns,
                         spread, mid, imbalance, trade_size_est);

    /* Console (optional; disable for cleaner collection runs) */
    printf("[%-6s] price=%.2f bid=%.2f ask=%.2f vol24h=%.4f seq=%" PRIu64
           " time=%s lat=%.2fms\n",
           s->product_id, s->price, s->best_bid, s->best_ask, s->volume_24h,
           s->last_seq, s->time_iso[0] ? s->time_iso : "(n/a)",
           s->last_ticker_latency_ms);
    fflush(stdout);
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
                  "\"type\":\"subscribe\","
                  "\"product_ids\":[\"" PRODUCT_ID "\"],"
                  "\"channels\":[\"ticker\",\"heartbeat\"]"
                "}";

            unsigned char buf[LWS_PRE + 256];
            size_t sub_len = strlen(sub);
            if (LWS_PRE + sub_len >= sizeof(buf)) return -1;

            memcpy(&buf[LWS_PRE], sub, sub_len);
            int n = lws_write(wsi, &buf[LWS_PRE], sub_len, LWS_WRITE_TEXT);
            if (n < 0) return -1;

            st->subscribed = 1;
            reset_rx(st);

            fprintf(stderr, "Connected; subscribed to %s ticker + heartbeat.\n", PRODUCT_ID);
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
            request_stop();
            break;

        case LWS_CALLBACK_CLIENT_CLOSED:
            fprintf(stderr, "Connection closed.\n");
            request_stop();
            break;

        default:
            break;
    }

    return 0;
}

/* ---------- Public entrypoint ---------- */

int cb_ws_run_eth_ticker(const char *endpoint_wss,
                         const char *csv_path,
                         eth_stats_t *out_stats,
                         volatile sig_atomic_t *stop_flag) {
    if (!endpoint_wss || !*endpoint_wss) endpoint_wss = DEFAULT_ENDPOINT;
    if (!out_stats) return 2;

    memset(out_stats, 0, sizeof(*out_stats));
    strncpy(out_stats->product_id, PRODUCT_ID, sizeof(out_stats->product_id) - 1);

    g_default_stop_flag = 0;
    g_stop_flag = stop_flag ? stop_flag : &g_default_stop_flag;

    if (!stop_flag) {
        signal(SIGINT, on_sigint);
        signal(SIGTERM, on_sigint);
    }

    app_state_t st;
    memset(&st, 0, sizeof(st));
    st.stats = out_stats;

    if (csv_open(&st, csv_path) != 0) {
        return 5;
    }

    struct lws_protocols protocols[] = {
        { "cb-proto", ws_callback, 0, 8192, 0, NULL, 0 },
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

    fprintf(stderr, "Connecting to %s ... (Ctrl+C to stop)\n", endpoint_wss);

    while (!*g_stop_flag) {
        lws_service(ctx, 200);
    }

    free(st.rx_buf);
    lws_context_destroy(ctx);
    csv_close(&st);
    return 0;
}

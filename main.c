#include "cb_ws.h"
#include <stdio.h>
#include <string.h>

typedef struct ticker_task {
    const char *endpoint;
    const char *csv_path;
    eth_stats_t *stats;
    volatile sig_atomic_t *stop_flag;
    int (*runner)(const char *, const char *, eth_stats_t *, volatile sig_atomic_t *);
    int rc;
} ticker_task_t;

static volatile sig_atomic_t g_stop_flag = 0;

static void on_sigint_main(int sig) {
    (void)sig;
    g_stop_flag = 1;
}

static void *ticker_thread(void *arg) {
    ticker_task_t *task = (ticker_task_t *)arg;
    task->rc = task->runner(task->endpoint, task->csv_path, task->stats, task->stop_flag);
    return NULL;
}

int main(int argc, char **argv) {
    const char *exchange = "coinbase";
    int argi = 1;

    if (argc > 1 && (strcmp(argv[1], "kraken") == 0 || strcmp(argv[1], "coinbase") == 0 || strcmp(argv[1], "both") == 0)) {
        exchange = argv[1];
        argi++;
    }

    if (strcmp(exchange, "both") == 0) {
        const char *cb_endpoint = (argc > argi) ? argv[argi] : "wss://ws-feed.exchange.coinbase.com";
        const char *cb_csv_path = (argc > argi + 1) ? argv[argi + 1] : "eth_ticker.csv";
        const char *kr_endpoint = (argc > argi + 2) ? argv[argi + 2] : "wss://ws.kraken.com";
        const char *kr_csv_path = (argc > argi + 3) ? argv[argi + 3] : "eth_ticker_kraken.csv";

        signal(SIGINT, on_sigint_main);
        signal(SIGTERM, on_sigint_main);

        eth_stats_t cb_stats;
        eth_stats_t kr_stats;
        ticker_task_t tasks[2] = {
            { cb_endpoint, cb_csv_path, &cb_stats, &g_stop_flag, cb_ws_run_eth_ticker, 0 },
            { kr_endpoint, kr_csv_path, &kr_stats, &g_stop_flag, kraken_ws_run_eth_ticker, 0 }
        };

        pthread_t cb_thread;
        pthread_t kr_thread;
        pthread_create(&cb_thread, NULL, ticker_thread, &tasks[0]);
        pthread_create(&kr_thread, NULL, ticker_thread, &tasks[1]);

        pthread_join(cb_thread, NULL);
        pthread_join(kr_thread, NULL);

        fprintf(stderr, "\nCoinbase Shutdown: updates=%llu last_price=%.2f last_seq=%llu\n",
                (unsigned long long)cb_stats.updates,
                cb_stats.price,
                (unsigned long long)cb_stats.last_seq);
        fprintf(stderr, "Kraken Shutdown: updates=%llu last_price=%.2f last_seq=%llu\n",
                (unsigned long long)kr_stats.updates,
                kr_stats.price,
                (unsigned long long)kr_stats.last_seq);

        return tasks[0].rc != 0 ? tasks[0].rc : tasks[1].rc;
    }

    const char *endpoint = (argc > argi) ? argv[argi]
        : (strcmp(exchange, "kraken") == 0
            ? "wss://ws.kraken.com"
            : "wss://ws-feed.exchange.coinbase.com");
    const char *csv_path = (argc > argi + 1) ? argv[argi + 1]
        : (strcmp(exchange, "kraken") == 0 ? "eth_ticker_kraken.csv" : "eth_ticker.csv");

    eth_stats_t stats;

    int rc = 0;
    if (strcmp(exchange, "kraken") == 0) {
        rc = kraken_ws_run_eth_ticker(endpoint, csv_path, &stats, NULL);
    } else {
        rc = cb_ws_run_eth_ticker(endpoint, csv_path, &stats, NULL);
    }

    fprintf(stderr, "\nShutdown: updates=%llu last_price=%.2f last_seq=%llu\n",
            (unsigned long long)stats.updates,
            stats.price,
            (unsigned long long)stats.last_seq);

    return rc;
}

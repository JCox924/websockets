#include "cb_ws.h"
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
    const char *exchange = "coinbase";
    int argi = 1;

    if (argc > 1 && (strcmp(argv[1], "kraken") == 0 || strcmp(argv[1], "coinbase") == 0)) {
        exchange = argv[1];
        argi++;
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
        rc = kraken_ws_run_eth_ticker(endpoint, csv_path, &stats);
    } else {
        rc = cb_ws_run_eth_ticker(endpoint, csv_path, &stats);
    }

    fprintf(stderr, "\nShutdown: updates=%llu last_price=%.2f last_seq=%llu\n",
            (unsigned long long)stats.updates,
            stats.price,
            (unsigned long long)stats.last_seq);

    return rc;
}

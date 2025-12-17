#include "cb_ws.h"
#include <stdio.h>

int main(int argc, char **argv) {
    const char *endpoint = (argc > 1) ? argv[1] : "wss://ws-feed.exchange.coinbase.com";
    const char *csv_path = (argc > 2) ? argv[2] : "eth_ticker.csv";

    eth_stats_t stats;
    int rc = cb_ws_run_eth_ticker(endpoint, csv_path, &stats);

    fprintf(stderr, "\nShutdown: updates=%llu last_price=%.2f last_seq=%llu\n",
            (unsigned long long)stats.updates,
            stats.price,
            (unsigned long long)stats.last_seq);

    return rc;
}

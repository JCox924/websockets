//
// Created by Joshua Cox on 12/15/25.
//
#ifndef CB_WS_H
#define CB_WS_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct eth_stats {
        char    product_id[16];     // e.g., "ETH-USD"
        double  price;              // last trade price
        double  best_bid;
        double  best_ask;
        double best_bid_size;
        double best_ask_size;
        double  volume_24h;
        char    time_iso[40];       // RFC3339-ish timestamp from feed (string)
        uint64_t last_seq;          // last seen sequence (if present)
        uint64_t updates;
        double last_ticker_latency_ms;

        uint64_t heartbeats;
        double last_hb_latency_ms;
        double last_hb_interarrival_ms;// count ticker updates processed
    } eth_stats_t;

    /**
     * Connects to Coinbase Exchange WebSocket feed and streams ticker updates for ETH-USD.
     * Blocks in the event loop until SIGINT or connection failure.
     *
     * Returns 0 on normal shutdown, non-zero on error.
     */
    int cb_ws_run_eth_ticker(const char *endpoint_wss, const char *csv_path, eth_stats_t *out_stats);

    /**
     * Connects to Kraken WebSocket feed and streams ticker updates for ETH/USD.
     * Blocks in the event loop until SIGINT or connection failure.
     *
     * Returns 0 on normal shutdown, non-zero on error.
     */
    int kraken_ws_run_eth_ticker(const char *endpoint_wss, const char *csv_path, eth_stats_t *out_stats);

#ifdef __cplusplus
}
#endif

#endif /* CB_WS_H */

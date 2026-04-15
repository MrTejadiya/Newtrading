#include <iostream>
#include <iomanip>
#include "HistoricalDataManager.hpp"
#include <climits>

using namespace upstox::v3;

int main(int argc, char** argv) {
    std::string dir = "data/aggregates";
    if (argc > 1) dir = argv[1];

    HistoricalDataManager mgr;
    bool ok = mgr.load_directory(dir);
    if (!ok) {
        std::cerr << "Failed to load any files from: " << dir << "\n";
        return 2;
    }

    auto instruments = mgr.list_instruments();
    std::cout << "Loaded instruments: " << instruments.size() << "\n";
    for (auto &k : instruments) std::cout << " - " << k << "\n";

    // Use instrument we loaded (exact key may be 'NSE_EQ|INE002A01018')
    std::string key = "NSE_EQ|INE002A01018";
    if (instruments.empty()) return 0;
    if (std::find(instruments.begin(), instruments.end(), key) == instruments.end()) {
        key = instruments.front();
        std::cout << "Using first available instrument: " << key << "\n";
    }

    // pick last 7 days range for sample
    // naive: find last timestamp from data
    auto rows = mgr.get_by_instrument(key);
    if (!rows) {
        std::cerr << "No rows for " << key << "\n";
        return 1;
    }
    if (rows->empty()) {
        std::cerr << "Empty rows for " << key << "\n";
        return 1;
    }

    long long last_ms = 0;
    long long first_ms = LLONG_MAX;
    for (auto &r : *rows) {
        if (r.contains("timestamp_ms")) {
            long long t = r["timestamp_ms"].get<long long>();
            if (t > last_ms) last_ms = t;
            if (t < first_ms) first_ms = t;
        }
    }
    if (last_ms == 0) {
        std::cerr << "Rows do not have timestamp_ms field.\n";
        return 1;
    }

    long long one_day_ms = 24LL*60*60*1000;
    long long start = last_ms - 7*one_day_ms;
    long long end = last_ms;
    long long period = 60LL*60*1000; // hourly

    auto agg = mgr.aggregate_ohlc(key, start, end, period);
    std::cout << "Aggregated " << agg.size() << " buckets for " << key << "\n";
    std::cout << std::fixed << std::setprecision(2);
    for (size_t i=0;i<agg.size() && i<10;i++) {
        auto &b = agg[i];
        std::cout << "start_ms="<<b.start_ms<<" open="<<b.open<<" high="<<b.high<<" low="<<b.low<<" close="<<b.close<<" vol="<<b.volume<<"\n";
    }

    return 0;
}

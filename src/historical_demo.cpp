#include <iostream>
#include "InstrumentKeyManager.hpp"
#include "HistoricalDataManager.hpp"

int main_demo(int argc, char** argv) {
    std::cout << "Historical demo starting\n";

    // Load instruments
    std::string filepath = (argc > 1) ? std::string(argv[1]) : std::string("data/complete.json");
    upstox::v3::InstrumentKeyManager instrument_manager;
    if (!instrument_manager.load_from_json(filepath)) {
        std::cerr << "Failed to load instrument data from: " << filepath << "\n";
    }

    // Demo HistoricalDataManager
    std::string hist_dir = "data/quotes";
    if (argc > 2) hist_dir = argv[2];

    upstox::v3::HistoricalDataManager hdm;
    bool loaded = hdm.load_directory(hist_dir, /*index_only=*/false);
    if (!loaded) {
        std::cout << "No historical data loaded from " << hist_dir << "\n";
        return 0;
    }

    auto instruments = hdm.list_instruments();
    std::cout << "Loaded instruments (" << instruments.size() << "):\n";
    for (auto& ik : instruments) std::cout << " - " << ik << "\n";

    // If instrument provided as third argument, show a 1-minute OHLC sample
    if (!instruments.empty()) {
        const auto& sample = instruments.front();
        auto rows = hdm.get_by_instrument(sample);
        if (rows && !rows->empty()) {
            long long start_ms = 0, end_ms = 0;
            for (auto& r : *rows) {
                if (r.contains("time") && r["time"].is_number()) {
                    long long t = r["time"].get<long long>();
                    if (start_ms==0 || t < start_ms) start_ms = t;
                    if (end_ms==0 || t > end_ms) end_ms = t;
                }
            }
            if (start_ms && end_ms && end_ms > start_ms) {
                auto ohlc = hdm.aggregate_ohlc(sample, start_ms, end_ms, 60000);
                std::cout << "OHLC buckets: " << ohlc.size() << "\n";
                for (auto& o : ohlc) {
                    std::cout << "start=" << o.start_ms << " open=" << o.open << " high=" << o.high << " low=" << o.low << " close=" << o.close << " vol=" << o.volume << "\n";
                }
            }
        }
    }

    return 0;
}

int main(int argc, char** argv) { return main_demo(argc, argv); }

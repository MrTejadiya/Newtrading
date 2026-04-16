#include <iostream>
#include "InstrumentKeyManager.hpp"

int main_demo(int argc, char** argv) {
    std::cout << "Historical demo starting (InstrumentKeyManager only)\n";

    // Load instruments
    std::string filepath = (argc > 1) ? std::string(argv[1]) : std::string("data/complete.json");
    upstox::v3::InstrumentKeyManager instrument_manager;
    if (!instrument_manager.load_from_json(filepath)) {
        std::cerr << "Failed to load instrument data from: " << filepath << "\n";
        return 1;
    }

    // Show info for a sample instrument: prefer a key passed as second arg
    std::string sample = (argc > 2) ? std::string(argv[2]) : std::string("NSE_EQ|INE002A01018");
    auto info = instrument_manager.get_by_key(sample);
    if (info) {
        std::cout << "Found instrument: " << info->instrument_key << " - " << info->name << " (" << info->tradingsymbol << ")\n";
    } else {
        std::cout << "Instrument key not found: " << sample << "\n";
    }

    return 0;
}

int main(int argc, char** argv) { return main_demo(argc, argv); }

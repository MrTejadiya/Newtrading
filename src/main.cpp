#include <iostream>
#include "InstrumentKeyManager.hpp"

int main(int argc, char** argv) {
    std::cout << "New Trading system started\n";
    std::cout << "Enforcing Upstox API Version: " << upstox::v3::API_VERSION << "\n";

    // Production: accept a path to the instruments JSON or fall back to the shipped data
    std::string filepath = (argc > 1) ? std::string(argv[1]) : std::string("data/complete.json");

    upstox::v3::InstrumentKeyManager instrument_manager;
    if (!instrument_manager.load_from_json(filepath)) {
        std::cerr << "Failed to load instrument data from: " << filepath << "\n";
        return 1;
    }

    // Normal startup does not run self-tests; regression tests live under tests/
    return 0;
}

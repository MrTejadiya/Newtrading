#include <iostream>
#include "InstrumentKeyManager.hpp"

int main(int argc, char** argv) {
    std::cout << "New Trading system started\n";
    std::cout << "Enforcing Upstox API Version: " << upstox::v3::API_VERSION << "\n";

    // Keep main minimal. See `historical_demo` executable for a demo that
    // loads historical data and prints instruments.
    return 0;
}

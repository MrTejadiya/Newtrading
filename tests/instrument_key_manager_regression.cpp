#include <iostream>
#include <fstream>
#include <cassert>
#include "../src/InstrumentKeyManager.hpp"

using namespace upstox::v3;

void write_sample_json(const std::string& path) {
    std::ofstream f(path);
    f << R"([
        {
            "instrument_key": "NSE_EQ|INE123456789",
            "exchange_token": "1234",
            "tradingsymbol": "RELIANCE",
            "name": "RELIANCE INDUSTRIES LTD",
            "exchange": "NSE_EQ",
            "instrument_type": "EQUITY",
            "tick_size": 0.05,
            "lot_size": 1
        },
        {
            "instrument_key": "NSE_EQ|INE987654321",
            "exchange_token": "5678",
            "tradingsymbol": "TCS",
            "name": "TATA CONSULTANCY SERVICES",
            "exchange": "NSE_EQ",
            "instrument_type": "EQUITY",
            "tick_size": 0.05,
            "lot_size": 1
        }
    ])";
}

int main() {
    const std::string tmp = "tests_tmp.json";
    write_sample_json(tmp);

    InstrumentKeyManager mgr;
    bool loaded = mgr.load_from_json(tmp);
    assert(loaded && "Failed to load sample JSON in regression test");

    auto r = mgr.get_by_symbol("RELIANCE");
    assert(r && r->instrument_key == "NSE_EQ|INE123456789");

    auto tcs = mgr.get_by_key("NSE_EQ|INE987654321");
    assert(tcs && tcs->tradingsymbol == "TCS");

    auto none = mgr.get_by_symbol("UNKNOWN_SYMBOL");
    assert(!none && "Unexpectedly found UNKNOWN_SYMBOL");

    std::remove(tmp.c_str());
    std::cout << "Regression test passed\n";
    return 0;
}

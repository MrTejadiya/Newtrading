// GoogleTest-based regression for InstrumentKeyManager
#include <gtest/gtest.h>
#include <fstream>
#include "../src/InstrumentKeyManager.hpp"

using namespace upstox::v3;

static void write_sample_json(const std::string& path) {
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

TEST(InstrumentKeyManagerRegression, BasicLoadAndLookup) {
    const std::string tmp = "tests_tmp.json";
    write_sample_json(tmp);

    InstrumentKeyManager mgr;
    EXPECT_TRUE(mgr.load_from_json(tmp));

    auto r = mgr.get_by_symbol("RELIANCE");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->instrument_key, "NSE_EQ|INE123456789");

    auto tcs = mgr.get_by_key("NSE_EQ|INE987654321");
    ASSERT_TRUE(tcs.has_value());
    EXPECT_EQ(tcs->tradingsymbol, "TCS");

    auto none = mgr.get_by_symbol("UNKNOWN_SYMBOL");
    EXPECT_FALSE(none.has_value());

    std::remove(tmp.c_str());
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include "../src/HistoricalDataManager.hpp"

using namespace upstox::v3;
namespace fs = std::filesystem;

static void write_sample_json(const fs::path& p, const std::string& instrument_key) {
    nlohmann::json j = nlohmann::json::array();
    j.push_back({{"time", 1700000000000LL}, {"open", 100.0}, {"close", 101.0}, {"volume", 10}});
    j.push_back({{"time", 1700000006000LL}, {"open", 101.0}, {"close", 102.0}, {"volume", 5}});
    std::ofstream f(p);
    f << j.dump(2);
    // write meta
    nlohmann::json m = {{"instrument_key", instrument_key}};
    std::ofstream f2(p.string() + ".meta.json");
    f2 << m.dump(2);
}

TEST(HistoricalDataManager, LoadAndQuery) {
    fs::path tmp = "data/test_historical";
    fs::create_directories(tmp);
    auto file = tmp / "NSE_EQ_INE123_1700000000000_1700000006000.json";
    write_sample_json(file, "NSE_EQ|INE123");

    HistoricalDataManager mgr;
    ASSERT_TRUE(mgr.load_directory(tmp.string(), /*index_only=*/false));

    auto list = mgr.list_instruments();
    EXPECT_NE(std::find(list.begin(), list.end(), "NSE_EQ|INE123"), list.end());

    auto rows = mgr.query_range("NSE_EQ|INE123", 1699999999000LL, 1700000010000LL);
    EXPECT_GE(rows.size(), 2);

    auto ohlc = mgr.aggregate_ohlc("NSE_EQ|INE123", 1700000000000LL, 1700000006000LL, 60000);
    // depending on bucket logic, expect at least 1 bucket
    EXPECT_GE(ohlc.size(), 1);

    // cleanup
    fs::remove(file);
    fs::remove(file.string() + ".meta.json");
    fs::remove_all(tmp);
}

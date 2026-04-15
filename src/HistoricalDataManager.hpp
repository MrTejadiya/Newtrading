#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <optional>
#include <nlohmann/json.hpp>

namespace upstox::v3 {

using json = nlohmann::json;

// Simple manager to load historical data files (JSON or TSV) written by the
// `scripts/download_upstox_history.py` script. Files are grouped by instrument
// key and kept in memory as vectors of JSON objects.
class HistoricalDataManager {
public:
    HistoricalDataManager() = default;

    // Load all compatible files from a directory. Returns true if at least one
    // file was loaded successfully. If index_only is true, only index files and
    // do not load contents into memory.
    bool load_directory(const std::string& dir_path, bool index_only = false);

    // Retrieve loaded data for an instrument key (exact match). If not found,
    // returns std::nullopt.
    std::optional<std::vector<json>> get_by_instrument(const std::string& instrument_key) const;

    // List all instruments currently loaded
    std::vector<std::string> list_instruments() const;

    // When used in index-only mode, this returns the list of files for an instrument
    std::vector<std::string> indexed_files_for(const std::string& instrument_key) const;

    // Query loaded or indexed data for a time range (epoch ms). Loads files on demand if needed.
    std::vector<json> query_range(const std::string& instrument_key, long long start_ms, long long end_ms) const;

    struct OHLC {
        long long start_ms;
        double open;
        double high;
        double low;
        double close;
        double volume;
    };

    // Aggregate OHLC for the instrument over the given period_ms buckets in the specified time range.
    std::vector<OHLC> aggregate_ohlc(const std::string& instrument_key, long long start_ms, long long end_ms, long long period_ms) const;

private:
    std::unordered_map<std::string, std::vector<json>> data_;
    // If index_only or for streaming, map instrument -> list of files (absolute paths)
    std::unordered_map<std::string, std::vector<std::string>> index_map_;

    // Helpers
    static std::optional<std::string> find_instrument_in_json(const json& j);
    static std::vector<json> parse_tsv_file(const std::filesystem::path& p);
    static std::string instrument_from_filename(const std::string& stem);
};

} // namespace upstox::v3

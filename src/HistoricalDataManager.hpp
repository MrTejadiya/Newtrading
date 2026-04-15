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
    // file was loaded successfully.
    bool load_directory(const std::string& dir_path);

    // Retrieve loaded data for an instrument key (exact match). If not found,
    // returns std::nullopt.
    std::optional<std::vector<json>> get_by_instrument(const std::string& instrument_key) const;

    // List all instruments currently loaded
    std::vector<std::string> list_instruments() const;

private:
    std::unordered_map<std::string, std::vector<json>> data_;

    // Helpers
    static std::optional<std::string> find_instrument_in_json(const json& j);
    static std::vector<json> parse_tsv_file(const std::filesystem::path& p);
    static std::string instrument_from_filename(const std::string& stem);
};

} // namespace upstox::v3

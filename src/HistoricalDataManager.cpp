#include "HistoricalDataManager.hpp"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <iostream>

using namespace std;
namespace fs = std::filesystem;

namespace upstox::v3 {

bool HistoricalDataManager::load_directory(const std::string& dir_path) {
    bool any = false;
    fs::path dir(dir_path);
    if (!fs::exists(dir) || !fs::is_directory(dir)) return false;

    // First pass: collect metadata mappings from <stem>.meta.json -> instrument_key
    std::unordered_map<std::string, std::string> meta_map;
    for (auto& entry : fs::directory_iterator(dir)) {
        if (!entry.is_regular_file()) continue;
        const auto p = entry.path();
        if (p.extension() == ".json") {
            auto name = p.filename().string();
            if (name.size() > 10 && name.find(".meta.json") != std::string::npos) {
                try {
                    std::ifstream f(p);
                    if (!f) continue;
                    json m;
                    f >> m;
                    if (m.contains("instrument_key") && m["instrument_key"].is_string()) {
                        std::string stem = p.stem().string();
                        // p.stem() on "foo.meta.json" returns "foo.meta" so remove trailing ".meta"
                        if (stem.size() > 5 && stem.substr(stem.size()-5) == ".meta") stem = stem.substr(0, stem.size()-5);
                        meta_map[stem] = m["instrument_key"].get<string>();
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Failed to read meta " << p << ": " << e.what() << "\n";
                }
            }
        }
    }

    // Second pass: load data files and use meta_map when available
    for (auto& entry : fs::directory_iterator(dir)) {
        if (!entry.is_regular_file()) continue;
        const auto p = entry.path();
        try {
            if (p.extension() == ".json") {
                // skip meta files
                if (p.filename().string().find(".meta.json") != std::string::npos) continue;
                std::ifstream f(p);
                if (!f) continue;
                json j;
                f >> j;
                // Determine instrument: prefer meta_map for this stem
                string stem = p.stem().string();
                // if filename was foo_meta.json stem may include extra parts; leave as-is
                auto itmeta = meta_map.find(stem);
                string instrument;
                if (itmeta != meta_map.end()) {
                    instrument = itmeta->second;
                } else {
                    auto ik = find_instrument_in_json(j);
                    instrument = ik.value_or(instrument_from_filename(stem));
                }
                if (j.is_array()) {
                    data_[instrument].insert(data_[instrument].end(), j.begin(), j.end());
                } else if (j.is_object() && j.contains("candles") && j["candles"].is_array()) {
                    data_[instrument].insert(data_[instrument].end(), j["candles"].begin(), j["candles"].end());
                } else {
                    data_[instrument].push_back(j);
                }
                any = true;
            } else if (p.extension() == ".tsv" || p.extension() == ".csv") {
                auto rows = parse_tsv_file(p);
                string stem = p.stem().string();
                auto itmeta = meta_map.find(stem);
                string instrument = (itmeta != meta_map.end()) ? itmeta->second : instrument_from_filename(stem);
                data_[instrument].insert(data_[instrument].end(), rows.begin(), rows.end());
                any = true;
            }
        } catch (const std::exception& e) {
            std::cerr << "Failed to load " << p << ": " << e.what() << "\n";
        }
    }
    return any;
}

optional<vector<json>> HistoricalDataManager::get_by_instrument(const std::string& instrument_key) const {
    if (auto it = data_.find(instrument_key); it != data_.end()) {
        return it->second;
    }
    return nullopt;
}

vector<string> HistoricalDataManager::list_instruments() const {
    vector<string> keys;
    keys.reserve(data_.size());
    for (auto& kv : data_) keys.push_back(kv.first);
    return keys;
}

optional<string> HistoricalDataManager::find_instrument_in_json(const json& j) {
    // common shapes: object.instrument_key, object[0].instrument_key, etc.
    if (j.is_object()) {
        if (j.contains("instrument_key") && j["instrument_key"].is_string()) return j["instrument_key"].get<string>();
        if (j.contains("meta") && j["meta"].is_object() && j["meta"].contains("instrument_key")) return j["meta"]["instrument_key"].get<string>();
    } else if (j.is_array() && !j.empty() && j[0].is_object()) {
        auto& first = j[0];
        if (first.contains("instrument_key") && first["instrument_key"].is_string()) return first["instrument_key"].get<string>();
    }
    return nullopt;
}

vector<json> HistoricalDataManager::parse_tsv_file(const fs::path& p) {
    vector<json> rows;
    std::ifstream f(p);
    if (!f) return rows;
    string header_line;
    if (!std::getline(f, header_line)) return rows;
    // split on tabs
    vector<string> keys;
    {
        std::istringstream hs(header_line);
        string k;
        while (std::getline(hs, k, '\t')) keys.push_back(k);
    }
    string line;
    while (std::getline(f, line)) {
        std::istringstream ls(line);
        string cell;
        json obj = json::object();
        size_t i = 0;
        while (std::getline(ls, cell, '\t')) {
            if (i < keys.size()) obj[keys[i]] = cell;
            ++i;
        }
        rows.push_back(obj);
    }
    return rows;
}

string HistoricalDataManager::instrument_from_filename(const string& stem) {
    // filenames were created by script as <instrument_key>_<start>_<end>.json
    // instrument_key had '|' replaced by '_'
    // We'll try to take the first underscore-separated token and replace '_' back to '|'
    auto pos = stem.find('_');
    string candidate = (pos == string::npos) ? stem : stem.substr(0, pos);
    // Heuristic: if it contains '_' convert first '_' to '|'? Actually replace last '_' groups -> attempt simple reverse of the script's replacement
    // The script replaced '|' with '_', but underscores may exist in instrument keys; we cannot reliably reverse. Return the candidate as-is.
    return candidate;
}

} // namespace upstox::v3

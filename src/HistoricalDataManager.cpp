#include "HistoricalDataManager.hpp"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <iostream>
#include <ctime>

using namespace std;
namespace fs = std::filesystem;

namespace upstox::v3 {

bool HistoricalDataManager::load_directory(const std::string& dir_path, bool index_only) {
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
                        // derive base stem: handle "<stem>.meta.json" and "<stem>.json.meta.json"
                        std::string name = p.filename().string();
                        std::string stem;
                        const std::string meta_suffix = ".meta.json";
                        if (name.size() > meta_suffix.size() && name.substr(name.size() - meta_suffix.size()) == meta_suffix) {
                            stem = name.substr(0, name.size() - meta_suffix.size());
                            // if the remaining stem ends with ".json" (from an earlier append), strip it
                            const std::string json_suffix = ".json";
                            if (stem.size() > json_suffix.size() && stem.substr(stem.size() - json_suffix.size()) == json_suffix) {
                                stem = stem.substr(0, stem.size() - json_suffix.size());
                            }
                        } else {
                            stem = p.stem().string();
                            if (stem.size() > 5 && stem.substr(stem.size()-5) == ".meta") stem = stem.substr(0, stem.size()-5);
                        }
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
                // If indexing only, just record the file path
                if (index_only) {
                    index_map_[instrument].push_back(p.string());
                } else {
                    if (j.is_array()) {
                        data_[instrument].insert(data_[instrument].end(), j.begin(), j.end());
                    } else if (j.is_object() && j.contains("candles") && j["candles"].is_array()) {
                        data_[instrument].insert(data_[instrument].end(), j["candles"].begin(), j["candles"].end());
                    } else {
                        data_[instrument].push_back(j);
                    }
                }
                any = true;
            } else if (p.extension() == ".tsv" || p.extension() == ".csv") {
                auto rows = parse_tsv_file(p);
                string stem = p.stem().string();
                auto itmeta = meta_map.find(stem);
                string instrument = (itmeta != meta_map.end()) ? itmeta->second : instrument_from_filename(stem);
                if (index_only) {
                    index_map_[instrument].push_back(p.string());
                } else {
                    data_[instrument].insert(data_[instrument].end(), rows.begin(), rows.end());
                }
                any = true;
            }
        } catch (const std::exception& e) {
            std::cerr << "Failed to load " << p << ": " << e.what() << "\n";
        }
    }
    return any;
}

static long long extract_timestamp_ms(const json& obj) {
    // Try common keys: time, timestamp, t
    if (obj.is_object()) {
        if (obj.contains("timestamp_ms")) {
            if (obj["timestamp_ms"].is_number_integer()) return obj["timestamp_ms"].get<long long>();
            if (obj["timestamp_ms"].is_string()) {
                try { return stoll(obj["timestamp_ms"].get<string>()); } catch(...) {}
            }
        }
        for (auto k : {"time", "timestamp", "t"}) {
            if (obj.contains(k)) {
                if (obj[k].is_number_integer()) return obj[k].get<long long>();
                if (obj[k].is_string()) {
                    try {
                        return stoll(obj[k].get<string>());
                    } catch (...) {}
                }
            }
        }
    }
    return 0;
}

vector<json> HistoricalDataManager::query_range(const std::string& instrument_key, long long start_ms, long long end_ms) const {
    vector<json> result;
    // If we have loaded data in memory, filter it
    if (auto it = data_.find(instrument_key); it != data_.end()) {
        for (const auto& obj : it->second) {
            long long ts = extract_timestamp_ms(obj);
            if (ts == 0) { result.push_back(obj); continue; }
            if (ts >= start_ms && ts <= end_ms) result.push_back(obj);
        }
        return result;
    }

    // Otherwise, attempt to read indexed files
    if (auto itf = index_map_.find(instrument_key); itf != index_map_.end()) {
        for (const auto& file : itf->second) {
            fs::path p(file);
            try {
                if (p.extension() == ".json") {
                    std::ifstream f(p);
                    if (!f) continue;
                    json j; f >> j;
                    vector<json> rows;
                    if (j.is_array()) rows.assign(j.begin(), j.end());
                    else if (j.is_object() && j.contains("candles") && j["candles"].is_array()) rows.assign(j["candles"].begin(), j["candles"].end());
                    else rows.push_back(j);
                    for (auto& r : rows) {
                        long long ts = extract_timestamp_ms(r);
                        if (ts==0) { result.push_back(r); continue; }
                        if (ts >= start_ms && ts <= end_ms) result.push_back(r);
                    }
                } else if (p.extension()==".tsv" || p.extension()==".csv") {
                    auto rows = parse_tsv_file(p);
                    for (auto& r : rows) {
                        long long ts = extract_timestamp_ms(r);
                        if (ts==0) { result.push_back(r); continue; }
                        if (ts >= start_ms && ts <= end_ms) result.push_back(r);
                    }
                }
            } catch (...) {}
        }
    }
    return result;
}

vector<string> HistoricalDataManager::indexed_files_for(const std::string& instrument_key) const {
    if (auto it = index_map_.find(instrument_key); it != index_map_.end()) return it->second;
    return {};
}

vector<HistoricalDataManager::OHLC> HistoricalDataManager::aggregate_ohlc(const std::string& instrument_key, long long start_ms, long long end_ms, long long period_ms) const {
    vector<OHLC> out;
    if (period_ms <= 0) return out;
    auto rows = query_range(instrument_key, start_ms, end_ms);
    // bucket rows by (ts - start_ms) / period_ms
    std::map<long long, vector<json>> buckets;
    for (auto& r : rows) {
        long long ts = extract_timestamp_ms(r);
        if (ts==0) continue;
        long long b = (ts - start_ms) / period_ms;
        buckets[b].push_back(r);
    }
    for (auto& kv : buckets) {
        const auto& vec = kv.second;
        if (vec.empty()) continue;
        OHLC o{};
        o.start_ms = start_ms + kv.first * period_ms;
        // assume price field exists as "close" and "open" or use first/last
        double first_price = 0, last_price = 0, hi = -1e300, lo = 1e300, vol = 0;
        for (size_t i=0;i<vec.size();++i) {
            const auto& obj = vec[i];
            auto get_num = [&](const json &x)->double{
                try {
                    if (x.is_number()) return x.get<double>();
                    if (x.is_string()) return stod(x.get<string>());
                } catch(...){}
                return 0.0;
            };
            double price = 0;
            if (obj.contains("close")) price = get_num(obj["close"]);
            else if (obj.contains("price")) price = get_num(obj["price"]);
            else if (obj.contains("c")) price = get_num(obj["c"]);
            if (i==0) first_price = price;
            last_price = price;
            hi = std::max(hi, price);
            lo = std::min(lo, price);
            if (obj.contains("volume")) vol += get_num(obj["volume"]);
        }
        o.open = first_price; o.close = last_price; o.high = hi; o.low = lo; o.volume = vol;
        out.push_back(o);
    }
    return out;
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
        // If the TSV provided a bucket or timestamp string, try to convert to epoch ms for querying
        auto try_parse_iso = [](const std::string &s) -> long long {
            if (s.empty()) return 0;
            // if pure digits, treat as epoch ms
            bool all_digits = true;
            for (char c: s) if (!isdigit((unsigned char)c)) { all_digits = false; break; }
            if (all_digits) {
                try { return std::stoll(s); } catch(...) { return 0; }
            }
            // parse YYYY-MM-DDTHH:MM:SS optionally with timezone +HH:MM or -HH:MM or Z
            int Y=0, M=0, D=0, h=0, m=0, sec=0;
            if (sscanf(s.c_str(), "%4d-%2d-%2dT%2d:%2d:%2d", &Y,&M,&D,&h,&m,&sec) < 6) return 0;
            // find timezone
            int off_sign = 0, off_h = 0, off_m = 0;
            const char* tz = strchr(s.c_str(), 'Z');
            const char* plus = strchr(s.c_str(), '+');
            const char* minus = strchr(s.c_str()+10, '-'); // avoid date hyphens
            const char* tzn = nullptr;
            if (tz) tzn = tz;
            else if (plus) tzn = plus;
            else if (minus) tzn = minus;
            if (tzn && (*tzn == '+' || *tzn == '-')) {
                off_sign = (*tzn == '+') ? 1 : -1;
                // parse +HH:MM
                int oh=0, om=0;
                if (sscanf(tzn+1, "%2d:%2d", &oh, &om) >= 1) { off_h = oh; off_m = om; }
            }

            std::tm tm{};
            tm.tm_year = Y - 1900;
            tm.tm_mon = M - 1;
            tm.tm_mday = D;
            tm.tm_hour = h;
            tm.tm_min = m;
            tm.tm_sec = sec;
            // use timegm to get epoch seconds from UTC-like tm
            time_t t = timegm(&tm);
            if (t == (time_t)-1) return 0;
            long long epoch = (long long)t * 1000LL;
            // if timezone present and it's +HH:MM, adjust (local time - offset = UTC)
            if (off_sign != 0) {
                long long off_ms = (off_h * 3600 + off_m * 60) * 1000LL;
                epoch -= off_sign * off_ms;
            }
            return epoch;
        };

        // common column names: bucket or timestamp
        if (obj.contains("bucket") && obj["bucket"].is_string()) {
            long long ms = try_parse_iso(obj["bucket"].get<string>());
            if (ms) obj["timestamp_ms"] = ms;
        } else if (obj.contains("timestamp") && obj["timestamp"].is_string()) {
            long long ms = try_parse_iso(obj["timestamp"].get<string>());
            if (ms) obj["timestamp_ms"] = ms;
        }

        rows.push_back(obj);
    }
    return rows;
}

string HistoricalDataManager::instrument_from_filename(const string& stem) {
    // filenames were created by script as <safe_key>_<...> where safe_key = instrument_key with '|' -> '_'
    // Attempt to reverse that safely by locating an ISIN-like token (starts with 'IN') or a numeric token (for FO ids)
    std::vector<std::string> parts;
    std::string cur;
    for (char c: stem) {
        if (c == '_') { parts.push_back(cur); cur.clear(); }
        else cur.push_back(c);
    }
    if (!cur.empty()) parts.push_back(cur);

    // find token that looks like ISIN (starts with IN) or is all digits
    int idx = -1;
    for (size_t i = 0; i < parts.size(); ++i) {
        const auto &t = parts[i];
        if (t.size() >= 2 && t.rfind("IN", 0) == 0) { idx = (int)i; break; }
        bool alldigits = !t.empty();
        for (char ch: t) if (!isdigit((unsigned char)ch)) { alldigits = false; break; }
        if (alldigits) { idx = (int)i; break; }
    }
    if (idx > 0) {
        // join parts[0..idx-1] with '_' to form left, and use '|' + parts[idx]
        std::string left = parts[0];
        for (int i = 1; i < idx; ++i) left += "_" + parts[i];
        return left + "|" + parts[idx];
    }

    // fallback: return entire stem as candidate (may already be safe_key)
    return stem;
}

} // namespace upstox::v3

#include "InstrumentKeyManager.hpp"
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace upstox::v3 {

bool InstrumentKeyManager::load_from_json(const std::string& filepath) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open " << filepath << "\n";
        return false;
    }

    json j;
    try {
        file >> j;
    } catch (const json::parse_error& e) {
        std::cerr << "JSON parse error: " << e.what() << "\n";
        return false;
    }

    if (!j.is_array()) {
        std::cerr << "Expected JSON array\n";
        return false;
    }

    for (const auto& item : j) {
        if (!item.is_object()) continue;

        InstrumentInfo info;
        
        // Safe string extraction helper
        auto get_string = [&item](const std::string& key) {
            return (item.contains(key) && item[key].is_string()) ? item[key].get<std::string>() : "";
        };

        info.instrument_key = get_string("instrument_key");
        info.exchange_token = get_string("exchange_token");
        info.tradingsymbol = get_string("tradingsymbol");
        info.name = get_string("name");
        info.exchange = get_string("exchange");
        info.instrument_type = get_string("instrument_type");

        if (item.contains("tick_size") && item["tick_size"].is_number()) {
            info.tick_size = item["tick_size"].get<double>();
        } else {
            info.tick_size = 0.0;
        }

        if (item.contains("lot_size") && item["lot_size"].is_number()) {
            info.lot_size = item["lot_size"].get<int>();
        } else {
            info.lot_size = 0;
        }

        // Only cache if instrument_key exists
        if (!info.instrument_key.empty()) {
            key_map_[info.instrument_key] = info;
            if (!info.tradingsymbol.empty()) {
                symbol_to_key_map_[info.tradingsymbol] = info.instrument_key;
            }
        }
    }

    return true;
}

std::optional<InstrumentInfo> InstrumentKeyManager::get_by_key(const std::string& instrument_key) const {
    if (auto it = key_map_.find(instrument_key); it != key_map_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<InstrumentInfo> InstrumentKeyManager::get_by_symbol(const std::string& symbol) const {
    if (auto it = symbol_to_key_map_.find(symbol); it != symbol_to_key_map_.end()) {
        return get_by_key(it->second);
    }
    return std::nullopt;
}

} // namespace upstox::v3
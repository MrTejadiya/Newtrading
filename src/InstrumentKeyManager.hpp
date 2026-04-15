#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <optional>
#include <string_view>

namespace upstox::v3 {

// Constants to enforce v3 API usage across the system
constexpr std::string_view API_VERSION = "v3";
constexpr std::string_view BASE_URL = "https://api.upstox.com/v3";

struct InstrumentInfo {
    std::string instrument_key;
    std::string exchange_token;
    std::string tradingsymbol;
    std::string name;
    std::string exchange;
    std::string instrument_type;
    double tick_size;
    int lot_size;
};

class InstrumentKeyManager {
public:
    InstrumentKeyManager() = default;

    // Load instruments from Upstox master JSON file
    bool load_from_json(const std::string& filepath);

    // Lookups
    std::optional<InstrumentInfo> get_by_key(const std::string& instrument_key) const;
    std::optional<InstrumentInfo> get_by_symbol(const std::string& symbol) const;

private:
    std::unordered_map<std::string, InstrumentInfo> key_map_;
    std::unordered_map<std::string, std::string> symbol_to_key_map_;
};

} // namespace upstox::v3
#pragma once
#include "kronosdb/core/key.hpp"
#include <cstdint>
#include <string>
#include <string_view>
#include <array>
#include <span>
#include <stdexcept>

namespace kronos {

enum class EventType : uint8_t {
    Trade     = 0,
    Quote     = 1,
    Add       = 2,
    Modify    = 3,
    Cancel    = 4,
    Tombstone = 0xFF,  // logical delete marker; filtered from query results
};

struct Event {
    uint64_t  timestamp_ns;
    uint32_t  symbol_id;
    uint64_t  sequence_id;
    EventType event_type;
    int64_t   price_ticks;
    int64_t   bid_px_ticks;
    int64_t   ask_px_ticks;
    uint32_t  quantity;
    uint32_t  bid_qty;
    uint32_t  ask_qty;
    uint64_t  order_id;

    // 8+4+8+1+8+8+8+4+4+4+8 = 65 bytes
    static constexpr size_t kEncodedSize = 65;

    Key key() const noexcept { return {symbol_id, timestamp_ns, sequence_id}; }

    std::array<uint8_t, kEncodedSize> encode() const noexcept;
    static Event decode(std::span<const uint8_t, kEncodedSize> data);

    static EventType   parse_event_type(std::string_view s);
    static std::string_view event_type_name(EventType t) noexcept;
};

} // namespace kronos

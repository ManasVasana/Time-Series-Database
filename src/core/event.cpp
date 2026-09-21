#include "kronosdb/core/event.hpp"
#include <stdexcept>
#include <cstring>

namespace kronos {

namespace {
// Little-endian helpers
template<typename T>
void write_le(uint8_t* p, T v) {
    for (size_t i = 0; i < sizeof(T); ++i)
        p[i] = static_cast<uint8_t>(v >> (8 * i));
}
template<typename T>
T read_le(const uint8_t* p) {
    T v = 0;
    for (size_t i = 0; i < sizeof(T); ++i)
        v |= static_cast<T>(p[i]) << (8 * i);
    return v;
}
} // anonymous namespace

std::array<uint8_t, Event::kEncodedSize> Event::encode() const noexcept {
    std::array<uint8_t, kEncodedSize> buf{};
    uint8_t* p = buf.data();
    write_le<uint64_t>(p,      timestamp_ns);   p += 8;
    write_le<uint32_t>(p,      symbol_id);      p += 4;
    write_le<uint64_t>(p,      sequence_id);    p += 8;
    *p++ = static_cast<uint8_t>(event_type);
    write_le<int64_t> (p,      price_ticks);    p += 8;
    write_le<int64_t> (p,      bid_px_ticks);   p += 8;
    write_le<int64_t> (p,      ask_px_ticks);   p += 8;
    write_le<uint32_t>(p,      quantity);       p += 4;
    write_le<uint32_t>(p,      bid_qty);        p += 4;
    write_le<uint32_t>(p,      ask_qty);        p += 4;
    write_le<uint64_t>(p,      order_id);       // p += 8
    return buf;
}

Event Event::decode(std::span<const uint8_t, kEncodedSize> data) {
    const uint8_t* p = data.data();
    Event e{};
    e.timestamp_ns  = read_le<uint64_t>(p);  p += 8;
    e.symbol_id     = read_le<uint32_t>(p);  p += 4;
    e.sequence_id   = read_le<uint64_t>(p);  p += 8;
    e.event_type    = static_cast<EventType>(*p++);
    e.price_ticks   = read_le<int64_t> (p);  p += 8;
    e.bid_px_ticks  = read_le<int64_t> (p);  p += 8;
    e.ask_px_ticks  = read_le<int64_t> (p);  p += 8;
    e.quantity      = read_le<uint32_t>(p);  p += 4;
    e.bid_qty       = read_le<uint32_t>(p);  p += 4;
    e.ask_qty       = read_le<uint32_t>(p);  p += 4;
    e.order_id      = read_le<uint64_t>(p);
    return e;
}

EventType Event::parse_event_type(std::string_view s) {
    if (s == "trade")     return EventType::Trade;
    if (s == "quote")     return EventType::Quote;
    if (s == "add")       return EventType::Add;
    if (s == "modify")    return EventType::Modify;
    if (s == "cancel")    return EventType::Cancel;
    if (s == "tombstone") return EventType::Tombstone;
    throw std::invalid_argument(std::string("unknown event_type: ") + std::string(s));
}

std::string_view Event::event_type_name(EventType t) noexcept {
    switch (t) {
        case EventType::Trade:     return "trade";
        case EventType::Quote:     return "quote";
        case EventType::Add:       return "add";
        case EventType::Modify:    return "modify";
        case EventType::Cancel:    return "cancel";
        case EventType::Tombstone: return "tombstone";
    }
    return "unknown";
}

} // namespace kronos

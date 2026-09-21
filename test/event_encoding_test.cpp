#include <gtest/gtest.h>
#include "kronosdb/core/event.hpp"
#include "kronosdb/core/key.hpp"
#include <cstring>

using namespace kronos;

TEST(KeyEncoding, RoundTrip) {
    Key k{42, 1746123456789012345ULL, 9999};
    auto enc = k.encode();
    auto dec = Key::decode(std::span<const uint8_t, Key::kEncodedSize>(enc.data(), enc.size()));
    EXPECT_EQ(k, dec);
}

TEST(KeyEncoding, LexicographicOrder) {
    Key a{1, 100, 0};
    Key b{1, 200, 0};
    Key c{2, 100, 0};
    auto ea = a.encode();
    auto eb = b.encode();
    auto ec = c.encode();
    // ea < eb < ec in byte order
    EXPECT_LT(std::memcmp(ea.data(), eb.data(), Key::kEncodedSize), 0);
    EXPECT_LT(std::memcmp(eb.data(), ec.data(), Key::kEncodedSize), 0);
}

TEST(EventEncoding, RoundTrip) {
    Event e{};
    e.timestamp_ns  = 1746123456789012345ULL;
    e.symbol_id     = 7;
    e.sequence_id   = 12345;
    e.event_type    = EventType::Quote;
    e.price_ticks   = 0;
    e.bid_px_ticks  = 1874200;
    e.ask_px_ticks  = 1874300;
    e.quantity      = 0;
    e.bid_qty       = 300;
    e.ask_qty       = 500;
    e.order_id      = 0;

    auto enc = e.encode();
    ASSERT_EQ(enc.size(), Event::kEncodedSize);

    auto dec = Event::decode(std::span<const uint8_t, Event::kEncodedSize>(enc.data(), enc.size()));

    EXPECT_EQ(dec.timestamp_ns,  e.timestamp_ns);
    EXPECT_EQ(dec.symbol_id,     e.symbol_id);
    EXPECT_EQ(dec.sequence_id,   e.sequence_id);
    EXPECT_EQ(dec.event_type,    e.event_type);
    EXPECT_EQ(dec.bid_px_ticks,  e.bid_px_ticks);
    EXPECT_EQ(dec.ask_px_ticks,  e.ask_px_ticks);
    EXPECT_EQ(dec.bid_qty,       e.bid_qty);
    EXPECT_EQ(dec.ask_qty,       e.ask_qty);
}

TEST(EventEncoding, AllEventTypes) {
    for (auto t : {EventType::Trade, EventType::Quote,
                   EventType::Add, EventType::Modify, EventType::Cancel}) {
        Event e{};
        e.event_type = t;
        auto enc = e.encode();
        auto dec = Event::decode(std::span<const uint8_t, Event::kEncodedSize>(enc.data(), enc.size()));
        EXPECT_EQ(dec.event_type, t);
    }
}

TEST(EventEncoding, ParseEventType) {
    EXPECT_EQ(Event::parse_event_type("trade"),  EventType::Trade);
    EXPECT_EQ(Event::parse_event_type("quote"),  EventType::Quote);
    EXPECT_EQ(Event::parse_event_type("add"),    EventType::Add);
    EXPECT_EQ(Event::parse_event_type("modify"), EventType::Modify);
    EXPECT_EQ(Event::parse_event_type("cancel"), EventType::Cancel);
    EXPECT_THROW(Event::parse_event_type("bogus"), std::invalid_argument);
}

TEST(EventEncoding, EventTypeName) {
    EXPECT_EQ(Event::event_type_name(EventType::Trade),  "trade");
    EXPECT_EQ(Event::event_type_name(EventType::Quote),  "quote");
    EXPECT_EQ(Event::event_type_name(EventType::Add),    "add");
    EXPECT_EQ(Event::event_type_name(EventType::Modify), "modify");
    EXPECT_EQ(Event::event_type_name(EventType::Cancel), "cancel");
}

TEST(EventEncoding, SignedFields) {
    Event e{};
    e.price_ticks   = -9999;
    e.bid_px_ticks  = INT64_MIN;
    e.ask_px_ticks  = INT64_MAX;
    auto enc = e.encode();
    auto dec = Event::decode(std::span<const uint8_t, Event::kEncodedSize>(enc.data(), enc.size()));
    EXPECT_EQ(dec.price_ticks,  e.price_ticks);
    EXPECT_EQ(dec.bid_px_ticks, e.bid_px_ticks);
    EXPECT_EQ(dec.ask_px_ticks, e.ask_px_ticks);
}

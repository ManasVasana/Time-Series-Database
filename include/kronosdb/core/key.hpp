#pragma once
#include <cstdint>
#include <compare>
#include <array>
#include <span>
#include <cstring>

namespace kronos {

// Primary key: (symbol_id, timestamp_ns, sequence_id) -- total 20 bytes
struct Key {
    uint32_t symbol_id;
    uint64_t timestamp_ns;
    uint64_t sequence_id;

    auto operator<=>(const Key&) const = default;
    bool operator==(const Key&) const = default;

    static constexpr size_t kEncodedSize = 20;

    // Big-endian encoding for byte-lexicographic comparison in on-disk structures
    std::array<uint8_t, kEncodedSize> encode() const {
        std::array<uint8_t, kEncodedSize> buf{};
        buf[0]  = (symbol_id >> 24) & 0xFF;
        buf[1]  = (symbol_id >> 16) & 0xFF;
        buf[2]  = (symbol_id >>  8) & 0xFF;
        buf[3]  = (symbol_id      ) & 0xFF;
        buf[4]  = (timestamp_ns >> 56) & 0xFF;
        buf[5]  = (timestamp_ns >> 48) & 0xFF;
        buf[6]  = (timestamp_ns >> 40) & 0xFF;
        buf[7]  = (timestamp_ns >> 32) & 0xFF;
        buf[8]  = (timestamp_ns >> 24) & 0xFF;
        buf[9]  = (timestamp_ns >> 16) & 0xFF;
        buf[10] = (timestamp_ns >>  8) & 0xFF;
        buf[11] = (timestamp_ns      ) & 0xFF;
        buf[12] = (sequence_id >> 56) & 0xFF;
        buf[13] = (sequence_id >> 48) & 0xFF;
        buf[14] = (sequence_id >> 40) & 0xFF;
        buf[15] = (sequence_id >> 32) & 0xFF;
        buf[16] = (sequence_id >> 24) & 0xFF;
        buf[17] = (sequence_id >> 16) & 0xFF;
        buf[18] = (sequence_id >>  8) & 0xFF;
        buf[19] = (sequence_id      ) & 0xFF;
        return buf;
    }

    static Key decode(std::span<const uint8_t, kEncodedSize> b) {
        Key k{};
        k.symbol_id =
            (static_cast<uint32_t>(b[0]) << 24) |
            (static_cast<uint32_t>(b[1]) << 16) |
            (static_cast<uint32_t>(b[2]) <<  8) |
             static_cast<uint32_t>(b[3]);
        k.timestamp_ns =
            (static_cast<uint64_t>(b[4])  << 56) |
            (static_cast<uint64_t>(b[5])  << 48) |
            (static_cast<uint64_t>(b[6])  << 40) |
            (static_cast<uint64_t>(b[7])  << 32) |
            (static_cast<uint64_t>(b[8])  << 24) |
            (static_cast<uint64_t>(b[9])  << 16) |
            (static_cast<uint64_t>(b[10]) <<  8) |
             static_cast<uint64_t>(b[11]);
        k.sequence_id =
            (static_cast<uint64_t>(b[12]) << 56) |
            (static_cast<uint64_t>(b[13]) << 48) |
            (static_cast<uint64_t>(b[14]) << 40) |
            (static_cast<uint64_t>(b[15]) << 32) |
            (static_cast<uint64_t>(b[16]) << 24) |
            (static_cast<uint64_t>(b[17]) << 16) |
            (static_cast<uint64_t>(b[18]) <<  8) |
             static_cast<uint64_t>(b[19]);
        return k;
    }
};

} // namespace kronos

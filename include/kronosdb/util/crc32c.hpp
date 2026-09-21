#pragma once
#include <array>
#include <cstddef>
#include <cstdint>

namespace kronos {

// CRC32C (Castagnoli, polynomial 0x82F63B78) — software implementation.
//
// Chaining: crc32c(buf2, n, prev) correctly continues from prev because the
// function XORs seed with 0xFFFFFFFF on entry, which undoes the finalization of
// the previous call and restores the running internal state.
//
// Thread safety: the lookup table is a function-local static initialized with
// a constexpr IIFE.  C++11 guarantees exactly-once, thread-safe initialization.

namespace detail {

inline const std::array<uint32_t, 256>& crc32c_table() {
    static const std::array<uint32_t, 256> kTable = []() {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t crc = i;
            for (int j = 0; j < 8; ++j)
                crc = (crc & 1u) ? ((crc >> 1) ^ 0x82F63B78u) : (crc >> 1);
            t[i] = crc;
        }
        return t;
    }();
    return kTable;
}

} // namespace detail

inline uint32_t crc32c(const void* data, size_t len, uint32_t seed = 0) noexcept {
    const auto& tbl = detail::crc32c_table();
    const auto*  p  = static_cast<const uint8_t*>(data);
    uint32_t crc    = seed ^ 0xFFFFFFFFu;
    while (len--) crc = (crc >> 8) ^ tbl[(crc ^ *p++) & 0xFFu];
    return crc ^ 0xFFFFFFFFu;
}

} // namespace kronos

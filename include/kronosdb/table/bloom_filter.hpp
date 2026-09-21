#pragma once
#include <cstdint>
#include <span>
#include <vector>

namespace kronos {

// Bloom filter using double hashing: h_i(x) = (h1(x) + i * h2(x)) % m
// h1 = FNV-1a, h2 = Murmur-finalizer mix.
//
// Serialized form: [k:4LE][m:4LE][bits: ceil(m/8) bytes]
// Stored in the SSTable filter block so it survives restarts.

class BloomFilter {
public:
    // Build a new filter expecting n_items with false-positive rate fp_rate.
    BloomFilter(uint32_t n_items, double fp_rate = 0.01);

    // Deserialize from encoded bytes.
    static BloomFilter decode(std::span<const uint8_t> data);

    void add(std::span<const uint8_t> key) noexcept;
    bool may_contain(std::span<const uint8_t> key) const noexcept;

    std::vector<uint8_t> encode() const;

    uint32_t hash_count()  const noexcept { return k_; }
    uint32_t bit_count()   const noexcept { return m_; }
    bool     empty()       const noexcept { return m_ == 0; }

    BloomFilter() = default;   // empty filter (m_==0 means may_contain always true)

private:

    uint32_t              k_{0};
    uint32_t              m_{0};
    std::vector<uint8_t>  bits_;

    static uint32_t fnv1a(std::span<const uint8_t> key) noexcept;
    static uint32_t murmur_mix(std::span<const uint8_t> key) noexcept;

    void set_bit(uint32_t pos) noexcept;
    bool get_bit(uint32_t pos) const noexcept;
};

} // namespace kronos

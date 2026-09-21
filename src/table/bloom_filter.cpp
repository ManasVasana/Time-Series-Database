#include "kronosdb/table/bloom_filter.hpp"
#include <cmath>
#include <cstring>
#include <stdexcept>

namespace kronos {

// ── helpers ──────────────────────────────────────────────────────────────────

static void write_u32(uint8_t* p, uint32_t v) {
    p[0] = (v      ) & 0xFF;
    p[1] = (v >>  8) & 0xFF;
    p[2] = (v >> 16) & 0xFF;
    p[3] = (v >> 24) & 0xFF;
}
static uint32_t read_u32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0])
        | (static_cast<uint32_t>(p[1]) <<  8)
        | (static_cast<uint32_t>(p[2]) << 16)
        | (static_cast<uint32_t>(p[3]) << 24);
}

// ── BloomFilter ───────────────────────────────────────────────────────────────

BloomFilter::BloomFilter(uint32_t n_items, double fp_rate) {
    if (n_items == 0) n_items = 1;
    // m = ceil(-n * ln(p) / (ln2)^2)
    const double ln2   = 0.693147180559945;
    const double ln2sq = ln2 * ln2;
    double m_d = std::ceil(-static_cast<double>(n_items) * std::log(fp_rate) / ln2sq);
    m_ = static_cast<uint32_t>(std::max(m_d, 8.0));
    // k = round(m * ln2 / n)
    double k_d = std::round(static_cast<double>(m_) * ln2 / static_cast<double>(n_items));
    k_ = static_cast<uint32_t>(std::max(k_d, 1.0));
    bits_.assign((m_ + 7) / 8, 0);
}

uint32_t BloomFilter::fnv1a(std::span<const uint8_t> key) noexcept {
    uint32_t hash = 2166136261u;
    for (uint8_t b : key) {
        hash ^= static_cast<uint32_t>(b);
        hash *= 16777619u;
    }
    return hash;
}

uint32_t BloomFilter::murmur_mix(std::span<const uint8_t> key) noexcept {
    uint32_t h = 0xDEADBEEFu;
    for (uint8_t b : key) {
        h ^= static_cast<uint32_t>(b);
        h  = (h ^ (h >> 16)) * 0x45d9f3bu;
        h  = (h ^ (h >> 16)) * 0x45d9f3bu;
        h ^= (h >> 16);
    }
    return h;
}

void BloomFilter::set_bit(uint32_t pos) noexcept {
    bits_[pos / 8] |= static_cast<uint8_t>(1u << (pos % 8));
}

bool BloomFilter::get_bit(uint32_t pos) const noexcept {
    return (bits_[pos / 8] >> (pos % 8)) & 1u;
}

void BloomFilter::add(std::span<const uint8_t> key) noexcept {
    if (m_ == 0) return;
    uint32_t h1 = fnv1a(key);
    uint32_t h2 = murmur_mix(key);
    for (uint32_t i = 0; i < k_; ++i)
        set_bit((h1 + i * h2) % m_);
}

bool BloomFilter::may_contain(std::span<const uint8_t> key) const noexcept {
    if (m_ == 0) return true;
    uint32_t h1 = fnv1a(key);
    uint32_t h2 = murmur_mix(key);
    for (uint32_t i = 0; i < k_; ++i)
        if (!get_bit((h1 + i * h2) % m_)) return false;
    return true;
}

// Serialized form: k(4LE) m(4LE) bits(ceil(m/8) bytes)
std::vector<uint8_t> BloomFilter::encode() const {
    size_t byte_count = (m_ + 7) / 8;
    std::vector<uint8_t> out(8 + byte_count);
    write_u32(out.data(),     k_);
    write_u32(out.data() + 4, m_);
    if (byte_count) std::memcpy(out.data() + 8, bits_.data(), byte_count);
    return out;
}

BloomFilter BloomFilter::decode(std::span<const uint8_t> data) {
    if (data.size() < 8)
        throw std::runtime_error("bloom filter data too small");
    BloomFilter f;
    f.k_ = read_u32(data.data());
    f.m_ = read_u32(data.data() + 4);
    size_t byte_count = (f.m_ + 7) / 8;
    if (data.size() < 8 + byte_count)
        throw std::runtime_error("bloom filter data truncated");
    f.bits_.assign(data.data() + 8, data.data() + 8 + byte_count);
    return f;
}

} // namespace kronos

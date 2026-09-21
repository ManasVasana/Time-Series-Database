#include <gtest/gtest.h>
#include "kronosdb/table/bloom_filter.hpp"
#include <string>
#include <vector>
#include <cstring>

using namespace kronos;

static std::span<const uint8_t> as_bytes(const std::string& s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

TEST(BloomFilterTest, ContainsAddedKeys) {
    BloomFilter f(1000, 0.01);
    std::vector<std::string> keys = {"AAPL", "MSFT", "NVDA", "TSLA", "GOOG"};
    for (auto& k : keys) f.add(as_bytes(k));
    for (auto& k : keys) EXPECT_TRUE(f.may_contain(as_bytes(k)));
}

TEST(BloomFilterTest, EmptyFilterAlwaysReturnsTrue) {
    BloomFilter f;
    EXPECT_TRUE(f.may_contain(as_bytes("anything")));
    EXPECT_TRUE(f.empty());
}

TEST(BloomFilterTest, FalsePositiveRateApproximate) {
    const int N = 10000;
    BloomFilter f(N, 0.01);

    // Insert N keys
    for (int i = 0; i < N; ++i) {
        std::string k = "key_inserted_" + std::to_string(i);
        f.add(as_bytes(k));
    }

    // Test N different keys that were never inserted
    int false_positives = 0;
    for (int i = 0; i < N; ++i) {
        std::string k = "key_absent_" + std::to_string(i);
        if (f.may_contain(as_bytes(k))) ++false_positives;
    }

    double fpr = static_cast<double>(false_positives) / N;
    // Target is 1%; allow 3x margin for statistical variation
    EXPECT_LT(fpr, 0.03) << "false positive rate " << fpr << " too high";
}

TEST(BloomFilterTest, EncodeDecodeRoundTrip) {
    BloomFilter f(500, 0.01);
    std::vector<std::string> keys = {"alpha", "beta", "gamma", "delta"};
    for (auto& k : keys) f.add(as_bytes(k));

    auto encoded = f.encode();
    EXPECT_GT(encoded.size(), 8u);

    auto decoded = BloomFilter::decode(std::span<const uint8_t>(encoded));

    EXPECT_EQ(decoded.hash_count(), f.hash_count());
    EXPECT_EQ(decoded.bit_count(),  f.bit_count());

    for (auto& k : keys)
        EXPECT_TRUE(decoded.may_contain(as_bytes(k)));
}

TEST(BloomFilterTest, LargeFilterCorrectness) {
    const int N = 100000;
    BloomFilter f(N, 0.01);
    for (int i = 0; i < N; ++i) {
        std::string k = std::to_string(i);
        f.add(as_bytes(k));
    }
    for (int i = 0; i < N; ++i) {
        std::string k = std::to_string(i);
        EXPECT_TRUE(f.may_contain(as_bytes(k)));
    }
}

TEST(BloomFilterTest, SerializedSizeReasonable) {
    // At 1% FPR: ~9.6 bits/element = ~1.2 bytes/element
    BloomFilter f(10000, 0.01);
    auto encoded = f.encode();
    size_t data_bytes = encoded.size() - 8;  // subtract k(4) + m(4) header
    double bytes_per_element = static_cast<double>(data_bytes) / 10000.0;
    EXPECT_LT(bytes_per_element, 2.0);
    EXPECT_GT(bytes_per_element, 0.5);
}

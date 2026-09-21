#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kronos {

// LRU block cache.
//
// Caches raw SSTable block bytes keyed by (file path, block byte offset).
// A cache hit avoids a pread() and block decode on every repeated scan.
// All methods are thread-safe.

class BlockCache {
public:
    explicit BlockCache(size_t max_bytes);

    // Returns pointer to cached data, or nullptr on miss.
    // Pointer valid until next put/evict on the same object.
    const std::vector<uint8_t>* get(const std::string& path,
                                     uint64_t offset) noexcept;

    void put(const std::string& path, uint64_t offset,
             std::vector<uint8_t> data);

    // Remove all blocks belonging to path (call when an SSTable is deleted).
    void evict(const std::string& path);

    size_t size_bytes()  const noexcept;
    size_t max_bytes()   const noexcept { return max_bytes_; }
    double hit_rate()    const noexcept;

    uint64_t hits()   const noexcept { return hits_.load(std::memory_order_relaxed); }
    uint64_t misses() const noexcept { return misses_.load(std::memory_order_relaxed); }

private:
    struct Key {
        std::string path;
        uint64_t    offset;
        bool operator==(const Key& o) const noexcept {
            return offset == o.offset && path == o.path;
        }
    };
    struct KeyHash {
        size_t operator()(const Key& k) const noexcept;
    };

    struct Entry {
        Key                   key;
        std::vector<uint8_t>  data;
    };

    using List    = std::list<Entry>;
    using ListIt  = List::iterator;
    using Index   = std::unordered_map<Key, ListIt, KeyHash>;

    size_t          max_bytes_;
    size_t          cur_bytes_{0};
    List            lru_;
    Index           index_;
    mutable std::mutex mu_;

    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};

    void evict_lru();
};

} // namespace kronos

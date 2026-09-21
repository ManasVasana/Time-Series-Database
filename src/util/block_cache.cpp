#include "kronosdb/util/block_cache.hpp"
#include <functional>

namespace kronos {

size_t BlockCache::KeyHash::operator()(const Key& k) const noexcept {
    // Combine string hash and offset hash
    size_t h1 = std::hash<std::string>{}(k.path);
    size_t h2 = std::hash<uint64_t>{}(k.offset);
    return h1 ^ (h2 * 0x9e3779b97f4a7c15ULL);
}

BlockCache::BlockCache(size_t max_bytes) : max_bytes_(max_bytes) {}

const std::vector<uint8_t>* BlockCache::get(const std::string& path,
                                              uint64_t offset) noexcept {
    std::lock_guard lock(mu_);
    auto it = index_.find({path, offset});
    if (it == index_.end()) {
        misses_.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    // Move to front (most recently used)
    lru_.splice(lru_.begin(), lru_, it->second);
    hits_.fetch_add(1, std::memory_order_relaxed);
    return &it->second->data;
}

void BlockCache::put(const std::string& path, uint64_t offset,
                     std::vector<uint8_t> data) {
    std::lock_guard lock(mu_);
    Key key{path, offset};

    auto it = index_.find(key);
    if (it != index_.end()) {
        cur_bytes_ -= it->second->data.size();
        lru_.erase(it->second);
        index_.erase(it);
    }

    size_t sz = data.size();
    lru_.push_front(Entry{key, std::move(data)});
    index_[key] = lru_.begin();
    cur_bytes_ += sz;

    while (cur_bytes_ > max_bytes_ && !lru_.empty())
        evict_lru();
}

void BlockCache::evict(const std::string& path) {
    std::lock_guard lock(mu_);
    auto it = lru_.begin();
    while (it != lru_.end()) {
        if (it->key.path == path) {
            cur_bytes_ -= it->data.size();
            index_.erase(it->key);
            it = lru_.erase(it);
        } else {
            ++it;
        }
    }
}

void BlockCache::evict_lru() {
    if (lru_.empty()) return;
    auto& back = lru_.back();
    cur_bytes_ -= back.data.size();
    index_.erase(back.key);
    lru_.pop_back();
}

size_t BlockCache::size_bytes() const noexcept {
    std::lock_guard lock(mu_);
    return cur_bytes_;
}

double BlockCache::hit_rate() const noexcept {
    uint64_t h = hits_.load(std::memory_order_relaxed);
    uint64_t m = misses_.load(std::memory_order_relaxed);
    uint64_t total = h + m;
    return total == 0 ? 0.0 : static_cast<double>(h) / static_cast<double>(total);
}

} // namespace kronos

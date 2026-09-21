#include "kronosdb/storage/memtable.hpp"

namespace kronos {

MemTable::MemTable(size_t flush_threshold_bytes)
    : flush_threshold_(flush_threshold_bytes),
      sl_(arena_, KeyCmp{}) {}

void MemTable::insert(const Event& e) {
    std::lock_guard lock(write_mu_);
    sl_.insert(e.key(), e);

    // Track min/max for may_contain checks without iterating
    const Key k = e.key();
    if (!has_entries_.load(std::memory_order_relaxed)) {
        std::lock_guard mm(minmax_mu_);
        first_key_ = k;
        last_key_  = k;
        has_entries_.store(true, std::memory_order_release);
    } else {
        std::lock_guard mm(minmax_mu_);
        if (k < first_key_) first_key_ = k;
        if (k > last_key_)  last_key_  = k;
    }
}

void MemTable::scan(const Key& start, const Key& end,
                    const std::function<bool(const Event&)>& cb) const {
    // Lock-free: uses acquire loads through the skip list iterator
    SkipList<Key, Event, KeyCmp>::Iterator it(sl_);
    it.seek(start);
    while (it.valid()) {
        if (it.key() > end) break;
        if (!cb(it.value())) break;
        it.next();
    }
}

std::vector<Event> MemTable::to_sorted_events() const {
    std::vector<Event> v;
    v.reserve(sl_.count());
    SkipList<Key, Event, KeyCmp>::Iterator it(sl_);
    it.seek_to_first();
    while (it.valid()) {
        v.push_back(it.value());
        it.next();
    }
    return v;
}

Key MemTable::min_key() const noexcept {
    if (!has_entries_.load(std::memory_order_acquire)) return {};
    std::lock_guard lock(minmax_mu_);
    return first_key_;
}

Key MemTable::max_key() const noexcept {
    if (!has_entries_.load(std::memory_order_acquire)) return {};
    std::lock_guard lock(minmax_mu_);
    return last_key_;
}

} // namespace kronos

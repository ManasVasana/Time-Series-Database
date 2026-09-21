#pragma once
#include "kronosdb/storage/arena.hpp"
#include "kronosdb/storage/skiplist.hpp"
#include "kronosdb/core/key.hpp"
#include "kronosdb/core/event.hpp"
#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <vector>

namespace kronos {

struct KeyCmp {
    bool operator()(const Key& a, const Key& b) const noexcept { return a < b; }
};

class MemTable {
public:
    explicit MemTable(size_t flush_threshold_bytes = 64 * 1024 * 1024);

    void insert(const Event& e);

    // Lock-free range scan [start, end]. Safe concurrent with insert().
    void scan(const Key& start, const Key& end,
              const std::function<bool(const Event&)>& cb) const;

    std::vector<Event> to_sorted_events() const;

    // size_bytes() returns actual arena usage — no estimation needed.
    size_t size_bytes()   const noexcept { return arena_.memory_used(); }
    size_t count()        const noexcept { return sl_.count(); }
    bool   should_flush() const noexcept { return arena_.memory_used() >= flush_threshold_; }
    bool   empty()        const noexcept { return sl_.empty(); }

    Key min_key() const noexcept;
    Key max_key() const noexcept;

private:
    size_t          flush_threshold_;
    Arena           arena_;                      // backing memory; freed on destructor
    SkipList<Key, Event, KeyCmp> sl_;            // built on arena_
    std::mutex write_mu_;                        // serializes concurrent insert() calls

    // Track min/max without scanning the list
    std::atomic<bool> has_entries_{false};
    Key first_key_{};   // set on first insert, never changes
    Key last_key_{};    // updated on each insert (monotonically increasing in practice)
    mutable std::mutex minmax_mu_;
};

} // namespace kronos

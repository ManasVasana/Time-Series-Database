#pragma once
#include "kronosdb/storage/arena.hpp"
#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <new>
#include <random>

namespace kronos {

// LevelDB-style skip list.
//
// Single-writer: insert() requires external serialization.
// Readers are lock-free: Iterator and contains() use acquire loads only.
//
// Arena must outlive the SkipList. All nodes freed in O(1) when the arena
// is destroyed — no per-node delete.

template <typename Key, typename Value, typename Cmp = std::less<Key>>
class SkipList {
    // Forward-declare Node here so that Iterator can use Node* as a data member
    // without the compiler introducing a stray forward declaration in the
    // enclosing namespace.
    struct Node;

public:
    explicit SkipList(Arena& arena, Cmp cmp = {});

    // Insert or update. If key already exists the value is replaced via an
    // atomic release-store so concurrent readers see either the old or the
    // new value, never a torn write.
    // Not thread-safe against concurrent insert() calls.
    void insert(const Key& k, const Value& v);

    bool contains(const Key& k) const noexcept;

    bool   empty() const noexcept { return count_.load(std::memory_order_relaxed) == 0; }
    size_t count() const noexcept { return count_.load(std::memory_order_relaxed); }

    // Forward iterator — lock-free, safe to use concurrently with insert().
    class Iterator {
    public:
        explicit Iterator(const SkipList& list) noexcept
            : list_(&list), node_(nullptr) {}

        bool         valid() const noexcept { return node_ != nullptr; }
        const Key&   key()   const noexcept { assert(valid()); return node_->key; }
        const Value& value() const noexcept {
            assert(valid());
            return *node_->val_ptr.load(std::memory_order_acquire);
        }

        void seek_to_first() noexcept { node_ = list_->head_->load_next(0); }

        void seek(const Key& target) noexcept {
            node_ = list_->find_first_ge(target);
        }

        void next() noexcept {
            assert(valid());
            node_ = node_->load_next(0);
        }

    private:
        const SkipList* list_;
        Node*           node_;
    };

private:
    static constexpr int kMaxHeight    = 12;
    static constexpr int kBranchFactor = 4;

    // Variable-height node allocated from the arena.
    // Layout: (key, val_ptr, height) followed immediately by next_[0..height-1].
    // sizeof(Node) includes next_[0] — allocate sizeof(Node)+(h-1)*sizeof(atomic<Node*>).
    struct Node {
        const Key                 key;
        std::atomic<const Value*> val_ptr;
        const int                 height;

        explicit Node(const Key& k, const Value* vp, int h)
            : key(k), val_ptr(vp), height(h) {
            for (int i = 0; i < h; ++i)
                next_[i].store(nullptr, std::memory_order_relaxed);
        }

        Node* load_next(int level) const noexcept {
            return next_[level].load(std::memory_order_acquire);
        }
        void store_next(int level, Node* n) noexcept {
            next_[level].store(n, std::memory_order_release);
        }
        void store_next_relaxed(int level, Node* n) noexcept {
            next_[level].store(n, std::memory_order_relaxed);
        }

        std::atomic<Node*> next_[1];
    };

    Arena&              arena_;
    Cmp                 cmp_;
    Node*               head_;
    std::atomic<int>    max_height_{1};
    std::atomic<size_t> count_{0};
    std::mt19937        rng_{42};

    Node* new_node(const Key& k, const Value* vp, int h) {
        size_t bytes = sizeof(Node) + static_cast<size_t>(h - 1) * sizeof(std::atomic<Node*>);
        return new (arena_.allocate_aligned(bytes)) Node(k, vp, h);
    }

    const Value* alloc_value(const Value& v) {
        return new (arena_.allocate_aligned(sizeof(Value))) Value(v);
    }

    int random_height() noexcept {
        int h = 1;
        while (h < kMaxHeight && (rng_() % static_cast<uint32_t>(kBranchFactor)) == 0)
            ++h;
        return h;
    }

    // Returns the greatest node with key strictly < target.
    // Fills prev[0..max_height-1] with the predecessor at each level.
    Node* find_prev(const Key& target, Node* prev[]) const noexcept {
        Node* x   = head_;
        int   lvl = max_height_.load(std::memory_order_relaxed) - 1;
        while (true) {
            Node* nx = x->load_next(lvl);
            if (nx != nullptr && cmp_(nx->key, target)) {
                x = nx;
            } else {
                if (prev) prev[lvl] = x;
                if (lvl == 0) return x;
                --lvl;
            }
        }
    }

    // Returns the first node with key >= target, or nullptr.
    Node* find_first_ge(const Key& target) const noexcept {
        Node* x   = head_;
        int   lvl = max_height_.load(std::memory_order_relaxed) - 1;
        while (true) {
            Node* nx = x->load_next(lvl);
            if (nx != nullptr && cmp_(nx->key, target)) {
                x = nx;
            } else {
                if (lvl == 0) return nx;
                --lvl;
            }
        }
    }
};

// ── implementation ────────────────────────────────────────────────────────────

template <typename K, typename V, typename C>
SkipList<K,V,C>::SkipList(Arena& arena, C cmp)
    : arena_(arena), cmp_(std::move(cmp)) {
    size_t bytes = sizeof(Node) + (kMaxHeight - 1) * sizeof(std::atomic<Node*>);
    head_ = new (arena_.allocate_aligned(bytes)) Node(K{}, nullptr, kMaxHeight);
}

template <typename K, typename V, typename C>
void SkipList<K,V,C>::insert(const K& k, const V& v) {
    Node* prev[kMaxHeight];
    Node* x = find_prev(k, prev)->load_next(0);

    // Key already exists — swap value pointer so readers see old or new, never torn.
    if (x != nullptr && !cmp_(x->key, k) && !cmp_(k, x->key)) {
        x->val_ptr.store(alloc_value(v), std::memory_order_release);
        return;
    }

    int h       = random_height();
    int cur_max = max_height_.load(std::memory_order_relaxed);
    if (h > cur_max) {
        for (int i = cur_max; i < h; ++i) prev[i] = head_;
        max_height_.store(h, std::memory_order_relaxed);
    }

    Node* n = new_node(k, alloc_value(v), h);
    for (int i = 0; i < h; ++i) {
        // Write n->next before linking n in: readers always see a complete node.
        n->store_next_relaxed(i, prev[i]->load_next(i));
        prev[i]->store_next(i, n);
    }
    count_.fetch_add(1, std::memory_order_relaxed);
}

template <typename K, typename V, typename C>
bool SkipList<K,V,C>::contains(const K& k) const noexcept {
    Node* x = find_first_ge(k);
    return x != nullptr && !cmp_(k, x->key) && !cmp_(x->key, k);
}

} // namespace kronos

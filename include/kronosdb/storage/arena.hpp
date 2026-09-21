#pragma once
#include <atomic>
#include <cstddef>
#include <new>
#include <vector>

namespace kronos {

// Bump-pointer arena. No per-object free; everything released in destructor.
// Not thread-safe — callers must synchronize.
class Arena {
public:
    Arena() = default;
    ~Arena() { for (auto* b : blocks_) ::operator delete[](b); }

    Arena(const Arena&)            = delete;
    Arena& operator=(const Arena&) = delete;

    char* allocate(size_t n) {
        if (n <= remaining_) {
            char* p  = ptr_;
            ptr_    += n;
            remaining_ -= n;
            return p;
        }
        return slow_alloc(n);
    }

    // Aligned to sizeof(void*) for std::atomic<Node*> members
    char* allocate_aligned(size_t n) {
        constexpr size_t a = sizeof(void*);
        return allocate((n + a - 1) & ~(a - 1));
    }

    size_t memory_used() const noexcept {
        return used_.load(std::memory_order_relaxed);
    }

private:
    static constexpr size_t kBlockSize = 4096;

    char*               ptr_       = nullptr;
    size_t              remaining_ = 0;
    std::atomic<size_t> used_{0};
    std::vector<char*>  blocks_;

    char* slow_alloc(size_t n) {
        size_t sz   = n > kBlockSize ? n : kBlockSize;
        char*  block = static_cast<char*>(::operator new[](sz));
        blocks_.push_back(block);
        used_.fetch_add(sz, std::memory_order_relaxed);
        ptr_       = block + n;
        remaining_ = sz - n;
        return block;
    }
};

} // namespace kronos

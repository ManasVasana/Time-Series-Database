#pragma once
#include "kronosdb/core/key.hpp"
#include "kronosdb/core/event.hpp"
#include <vector>
#include <span>
#include <cstdint>
#include <functional>

namespace kronos {

// Encoded block layout:
//   [record_count : 4]
//   [event_0 : 65] ... [event_N : 65]
//   [min_key : 20]
//   [max_key : 20]
//   [crc32c  :  4]
// Total overhead per block: 4 + 20 + 20 + 4 = 48 bytes

struct BlockMeta {
    Key      min_key;
    Key      max_key;
    uint32_t record_count;
    uint32_t encoded_size;  // bytes of the full encoded block
};

class BlockBuilder {
public:
    explicit BlockBuilder(size_t target_bytes = 64 * 1024);

    bool add(const Event& e);           // returns false if block is full
    bool empty() const { return events_.empty(); }
    size_t count() const { return events_.size(); }

    // Finalize the block and return its binary representation
    std::vector<uint8_t> finish() const;

    void reset();

    const std::vector<Event>& events() const { return events_; }

private:
    size_t          target_bytes_;
    std::vector<Event> events_;
};

class Block {
public:
    static Block decode(std::span<const uint8_t> data);

    const BlockMeta& meta() const { return meta_; }

    // Iterate events whose key is in [start, end]
    void scan(const Key& start, const Key& end,
              const std::function<bool(const Event&)>& cb) const;

    const std::vector<Event>& events() const { return events_; }

private:
    BlockMeta          meta_{};
    std::vector<Event> events_;
};

} // namespace kronos

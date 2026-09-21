#pragma once
#include "kronosdb/core/key.hpp"
#include <vector>
#include <span>
#include <cstdint>
#include <optional>

namespace kronos {

// Each entry maps the first key of a block to its byte offset and size within
// the SSTable file.
struct IndexEntry {
    Key      first_key;
    uint64_t block_offset;
    uint32_t block_size;

    // 20 (key) + 8 (offset) + 4 (size) = 32 bytes
    static constexpr size_t kEncodedSize = 32;
};

class IndexBlock {
public:
    void add(const Key& first_key, uint64_t offset, uint32_t size);

    // Returns the ENTRY INDEX (not byte offset) of the last entry whose
    // first_key <= query_key.  The caller must look up entries()[result] to
    // get the actual byte offset and size.  Returns nullopt if entries_ is empty.
    std::optional<size_t> find_first_block(const Key& query_key) const;

    // Returns entry indices of all blocks whose range might overlap [start, end].
    // All returned indices are valid for entries()[i].
    std::vector<size_t> blocks_in_range(const Key& start, const Key& end) const;

    std::vector<uint8_t> encode() const;
    static IndexBlock    decode(std::span<const uint8_t> data);

    const std::vector<IndexEntry>& entries() const { return entries_; }
    bool empty() const { return entries_.empty(); }

private:
    std::vector<IndexEntry> entries_;
};

} // namespace kronos

#pragma once
#include "kronosdb/core/key.hpp"
#include "kronosdb/core/event.hpp"
#include "kronosdb/table/block.hpp"
#include "kronosdb/table/index.hpp"
#include "kronosdb/table/bloom_filter.hpp"
#include <filesystem>
#include <functional>
#include <memory>
#include <vector>
#include <cstdint>

namespace kronos {

class BlockCache;

// SSTable file layout (v2 — adds filter block):
//
//   [data blocks ...]         sorted events, multiple 64 KB blocks
//   [filter block]            serialized BloomFilter
//   [index block]             sparse index: (first_key, offset, size) per block
//   [meta block]              event_count, data_bytes, min/max key, level, seq
//   [footer: 48 bytes]
//     filter_offset  : 8
//     filter_size    : 8
//     index_offset   : 8
//     index_size     : 8
//     meta_offset    : 8
//     meta_size      : 8
//     magic          : 4  = 0x4B535354 ('KSST')
//     version        : 4  = 2

static constexpr uint32_t kSSTableMagic    = 0x4B535354u;
static constexpr uint32_t kSSTableVersion  = 2u;
// Footer layout (56 bytes):
//   filter_offset(8) filter_size(8)
//   index_offset(8)  index_size(8)
//   meta_offset(8)   meta_size(8)
//   magic(4)         version(4)
static constexpr size_t kSSTableFooterSize = 56;

struct SSTableMeta {
    uint64_t event_count;
    uint64_t data_bytes;
    Key      min_key;
    Key      max_key;
    uint32_t level;
    uint64_t sequence;
};

struct SSTableInfo {
    std::filesystem::path path;
    SSTableMeta           meta;
    uint64_t              file_size;
};

class SSTableWriter {
public:
    SSTableWriter(std::filesystem::path path, uint32_t level, uint64_t sequence,
                  size_t block_target_bytes = 64 * 1024,
                  uint32_t expected_items   = 0,
                  double   bloom_fp_rate    = 0.01);
    ~SSTableWriter();

    void add(const Event& e);
    SSTableInfo finish();

private:
    std::filesystem::path path_;
    uint32_t              level_;
    uint64_t              sequence_;
    int                   fd_{-1};
    uint64_t              write_offset_{0};
    BlockBuilder          builder_;
    IndexBlock            index_;
    BloomFilter           filter_;
    uint64_t              event_count_{0};
    Key                   min_key_{};
    Key                   max_key_{};
    bool                  has_any_{false};

    void flush_block();
    void write_bytes(const void* data, size_t n);
};

class SSTableReader {
public:
    // cache may be nullptr — reads always hit disk in that case.
    SSTableReader(std::filesystem::path path,
                  BlockCache*           cache = nullptr);
    ~SSTableReader();

    const SSTableInfo& info() const { return info_; }

    void scan(const Key& start, const Key& end,
              const std::function<bool(const Event&)>& cb) const;

    bool may_contain(const Key& start, const Key& end) const noexcept;

    // Point lookup via bloom filter + binary index search.
    // Returns false immediately when the bloom filter rules it out.
    bool get(const Key& key, Event& out) const;

private:
    std::filesystem::path path_;
    int                   fd_{-1};
    SSTableInfo           info_;
    IndexBlock            index_;
    BloomFilter           filter_;
    BlockCache*           cache_{nullptr};

    void load_footer_and_index();
    Block read_block_at(uint64_t offset, uint32_t size) const;
};

} // namespace kronos

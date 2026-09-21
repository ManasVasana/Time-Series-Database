#pragma once
#include "kronosdb/core/event.hpp"
#include "kronosdb/util/symbol_table.hpp"
#include "kronosdb/storage/wal.hpp"
#include "kronosdb/storage/memtable.hpp"
#include "kronosdb/table/sstable.hpp"
#include "kronosdb/util/block_cache.hpp"
#include "kronosdb/db/compaction.hpp"
#include "kronosdb/util/metrics.hpp"
#include "kronosdb/db/query.hpp"
#include "kronosdb/db/recovery.hpp"
#include <filesystem>
#include <string>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <memory>
#include <cstdint>

namespace kronos {

struct DBConfig {
    std::filesystem::path path{"./data"};
    std::string           io_backend{"pread"};
    bool                  wal_enabled{true};
    size_t                memtable_flush_bytes{64 * 1024 * 1024};
    size_t                wal_segment_bytes{128 * 1024 * 1024};
    size_t                block_target_bytes{64 * 1024};
    size_t                sstable_target_bytes{64 * 1024 * 1024};
    uint32_t              max_immutable_memtables{4};
    bool                  background_compaction{true};
    size_t                block_cache_bytes{64 * 1024 * 1024};  // 64 MB block cache
    double                bloom_fp_rate{0.01};                  // 1% false-positive rate
};

struct AppendResult {
    uint64_t sequence_id;
    uint64_t wal_offset;
    double   append_latency_us;
};

class DB {
public:
    explicit DB(DBConfig cfg = {});
    ~DB();

    // Not copyable or movable — owns threads and open file descriptors
    DB(const DB&)            = delete;
    DB& operator=(const DB&) = delete;

    AppendResult append(const std::string& symbol, uint64_t timestamp_ns,
                        EventType event_type,
                        int64_t price_ticks, int64_t bid_px_ticks, int64_t ask_px_ticks,
                        uint32_t quantity, uint32_t bid_qty, uint32_t ask_qty,
                        uint64_t order_id);

    // Write a tombstone for (symbol, timestamp_ns, sequence_id).
    // Tombstones are filtered from query results and GC'd during compaction.
    AppendResult remove(const std::string& symbol,
                        uint64_t           timestamp_ns,
                        uint64_t           sequence_id);

    QueryResult query(const QueryRequest& req);

    uint64_t event_count()      const;
    uint64_t disk_usage_bytes() const;

    // All symbols that have been interned in this database (snapshot).
    std::vector<std::string> symbols() const;

    const DBConfig& config() const { return cfg_; }

    struct ActiveMemInfo { double active_mb; uint32_t immutable_count; };
    ActiveMemInfo          memtable_info() const;
    std::vector<LevelInfo> level_infos()   const;

    // Synchronous flush and compaction — used by benchmarks and tests
    void flush_now();
    void compact_now();

    // Idempotent; called by destructor
    void close();

private:
    DBConfig  cfg_;
    std::atomic<bool> closed_{false};

    std::unique_ptr<SymbolTable> symbols_;
    std::unique_ptr<WALWriter>   wal_;
    std::unique_ptr<BlockCache>  block_cache_;

    // Memtables
    std::unique_ptr<MemTable>              active_mem_;
    std::vector<std::unique_ptr<MemTable>> imm_mems_;
    mutable std::mutex                     mem_mu_;

    // SSTable levels: levels_[0] = L0, levels_[1] = L1, ...
    static constexpr int kMaxLevels = 4;
    std::vector<SSTableInfo> levels_[kMaxLevels];
    mutable std::mutex       levels_mu_;

    std::atomic<uint64_t> next_seq_{1};
    std::atomic<uint64_t> sst_seq_{1};

    // Background flush thread
    std::thread             flush_thread_;
    std::condition_variable flush_cv_;
    std::mutex              flush_mu_;
    std::atomic<bool>       stop_flush_{false};

    // Background compaction thread
    std::thread             compact_thread_;
    std::condition_variable compact_cv_;
    std::mutex              compact_mu_;
    std::atomic<bool>       stop_compact_{false};

    void open();
    void flush_loop();
    void compact_loop();
    void do_flush(std::unique_ptr<MemTable> mem, uint64_t seq);
    void do_compact_l0();
    bool needs_l0_compact() const;

    std::filesystem::path level_dir(int lvl)    const;
    uint64_t              next_sstable_seq();
};

} // namespace kronos

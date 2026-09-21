#include "kronosdb/db/db.hpp"
#include "kronosdb/util/metrics.hpp"
#include <chrono>
#include <algorithm>
#include <tuple>
#include <stdexcept>

namespace kronos {

static uint64_t now_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

// ---- DB ----

DB::DB(DBConfig cfg) : cfg_(std::move(cfg)) { open(); }

DB::~DB() { close(); }

void DB::open() {
    std::filesystem::create_directories(cfg_.path);

    symbols_     = std::make_unique<SymbolTable>(cfg_.path / "symbol_table.bin");
    block_cache_ = std::make_unique<BlockCache>(cfg_.block_cache_bytes);

    for (int l = 0; l < kMaxLevels; ++l)
        std::filesystem::create_directories(level_dir(l));

    // Load existing SSTables — errors on individual files are tolerated
    {
        std::lock_guard<std::mutex> lock(levels_mu_);
        for (int l = 0; l < kMaxLevels; ++l) {
            auto dir = level_dir(l);
            if (!std::filesystem::exists(dir)) continue;
            for (auto& entry : std::filesystem::directory_iterator(dir)) {
                if (!entry.is_regular_file()) continue;
                if (entry.path().extension() != ".sst") continue;
                try {
                    SSTableReader reader(entry.path());
                    levels_[l].push_back(reader.info());
                } catch (const std::exception&) {
                    // corrupted SSTable on startup — skip and continue
                }
            }
            std::sort(levels_[l].begin(), levels_[l].end(),
                [](const SSTableInfo& a, const SSTableInfo& b) {
                    return a.meta.sequence < b.meta.sequence;
                });
        }
    }

    active_mem_ = std::make_unique<MemTable>(cfg_.memtable_flush_bytes);

    // WAL recovery and writer setup
    if (cfg_.wal_enabled) {
        auto wal_dir = cfg_.path / "wal";

        if (std::filesystem::exists(wal_dir)) {
            // Recover records first, then open the writer
            size_t   torn_bytes       = 0;
            bool     torn_detected    = false;
            uint64_t clean_end_offset = 0;

            WALReader reader(wal_dir);
            auto records = reader.read_all(&torn_bytes, &torn_detected, &clean_end_offset);
            for (auto& rec : records)
                active_mem_->insert(rec.event);
            if (!records.empty())
                next_seq_.store(records.back().event.sequence_id + 1,
                                std::memory_order_relaxed);

            // Open writer, then truncate-and-seal if there was a torn tail.
            // This prevents new writes from landing after corrupt bytes.
            wal_ = std::make_unique<WALWriter>(wal_dir, cfg_.wal_segment_bytes);
            if (torn_detected) {
                wal_->truncate_and_seal(clean_end_offset);
            }
        } else {
            wal_ = std::make_unique<WALWriter>(wal_dir, cfg_.wal_segment_bytes);
        }
    }

    if (cfg_.background_compaction) {
        flush_thread_   = std::thread([this]() { flush_loop(); });
        compact_thread_ = std::thread([this]() { compact_loop(); });
    }
}

void DB::close() {
    // Use compare-exchange to ensure close() runs exactly once even if called
    // from multiple threads or from both the destructor and user code.
    bool expected = false;
    if (!closed_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel,
                                          std::memory_order_relaxed))
        return;

    stop_flush_.store(true, std::memory_order_release);
    flush_cv_.notify_all();
    if (flush_thread_.joinable()) flush_thread_.join();

    stop_compact_.store(true, std::memory_order_release);
    compact_cv_.notify_all();
    if (compact_thread_.joinable()) compact_thread_.join();

    // Flush whatever is in the active memtable
    {
        std::lock_guard<std::mutex> lock(mem_mu_);
        if (active_mem_ && !active_mem_->empty())
            imm_mems_.push_back(std::move(active_mem_));
    }
    // Drain all immutable memtables synchronously
    while (true) {
        std::unique_ptr<MemTable> to_flush;
        {
            std::lock_guard<std::mutex> lock(mem_mu_);
            if (imm_mems_.empty()) break;
            to_flush = std::move(imm_mems_.front());
            imm_mems_.erase(imm_mems_.begin());
        }
        do_flush(std::move(to_flush), next_sstable_seq());
    }

    if (wal_)     wal_->close();
    if (symbols_) symbols_->flush();
}

AppendResult DB::append(
        const std::string& symbol, uint64_t timestamp_ns,
        EventType event_type,
        int64_t price_ticks, int64_t bid_px_ticks, int64_t ask_px_ticks,
        uint32_t quantity, uint32_t bid_qty, uint32_t ask_qty,
        uint64_t order_id) {

    if (closed_.load(std::memory_order_acquire))
        throw std::runtime_error("DB is closed");

    uint64_t t0 = now_ns();

    uint32_t sym_id = symbols_->intern(symbol);
    uint64_t seq    = next_seq_.fetch_add(1, std::memory_order_relaxed);

    Event e{};
    e.timestamp_ns  = timestamp_ns;
    e.symbol_id     = sym_id;
    e.sequence_id   = seq;
    e.event_type    = event_type;
    e.price_ticks   = price_ticks;
    e.bid_px_ticks  = bid_px_ticks;
    e.ask_px_ticks  = ask_px_ticks;
    e.quantity      = quantity;
    e.bid_qty       = bid_qty;
    e.ask_qty       = ask_qty;
    e.order_id      = order_id;

    uint64_t wal_offset = 0;
    if (wal_) {
        auto res   = wal_->append(e);
        wal_offset = res.file_offset;
        auto& m    = global_metrics();
        m.wal_bytes_written.fetch_add(kWalHeaderSize + Event::kEncodedSize,
                                      std::memory_order_relaxed);
        m.wal_rate.add(kWalHeaderSize + Event::kEncodedSize);
    }

    {
        std::lock_guard<std::mutex> lock(mem_mu_);
        active_mem_->insert(e);

        if (active_mem_->should_flush()) {
            imm_mems_.push_back(std::move(active_mem_));
            active_mem_ = std::make_unique<MemTable>(cfg_.memtable_flush_bytes);
            flush_cv_.notify_one();
        }
    }

    auto& m = global_metrics();
    m.events_ingested_total.fetch_add(1, std::memory_order_relaxed);
    m.ingest_rate.add(1);

    {
        std::lock_guard<std::mutex> lock(mem_mu_);
        m.memtable_bytes.store(active_mem_->size_bytes(), std::memory_order_relaxed);
        m.immutable_memtable_count.store(
            static_cast<uint32_t>(imm_mems_.size()), std::memory_order_relaxed);
    }

    uint64_t lat_us = (now_ns() - t0) / 1000;
    m.append_latency_us.record(lat_us);

    return {seq, wal_offset, static_cast<double>(lat_us)};
}

AppendResult DB::remove(const std::string& symbol,
                         uint64_t           timestamp_ns,
                         uint64_t           sequence_id) {
    if (closed_.load(std::memory_order_acquire))
        throw std::runtime_error("DB is closed");

    uint64_t t0 = now_ns();

    // The tombstone MUST carry the exact same (symbol_id, timestamp_ns, sequence_id)
    // key as the event being deleted. This causes the skip list insert() to overwrite
    // the existing entry in-place. A new sequence_id would produce a different key
    // and leave the original event visible.
    uint32_t sym_id = symbols_->intern(symbol);
    Event e{};
    e.timestamp_ns = timestamp_ns;
    e.symbol_id    = sym_id;
    e.sequence_id  = sequence_id;
    e.event_type   = EventType::Tombstone;

    uint64_t wal_offset = 0;
    if (wal_) {
        auto res   = wal_->append(e);
        wal_offset = res.file_offset;
    }

    {
        std::lock_guard<std::mutex> lock(mem_mu_);
        active_mem_->insert(e);
        if (active_mem_->should_flush()) {
            imm_mems_.push_back(std::move(active_mem_));
            active_mem_ = std::make_unique<MemTable>(cfg_.memtable_flush_bytes);
            flush_cv_.notify_one();
        }
    }

    uint64_t lat_us = (now_ns() - t0) / 1000;
    return {sequence_id, wal_offset, static_cast<double>(lat_us)};
}

QueryResult DB::query(const QueryRequest& req) {
    if (closed_.load(std::memory_order_acquire))
        throw std::runtime_error("DB is closed");

    uint64_t t_total_start = now_ns();
    QueryResult result;

    uint64_t t_plan_start = now_ns();
    auto sym_id_opt = symbols_->lookup(req.symbol);
    result.stats.planning_us = (now_ns() - t_plan_start) / 1000;

    if (!sym_id_opt) {
        result.ok                 = true;
        result.stats.total_latency_us = (now_ns() - t_total_start) / 1000;
        return result;
    }

    uint32_t sym_id    = *sym_id_opt;
    Key      start_key = {sym_id, req.start_ns, 0};
    Key      end_key   = {sym_id, req.end_ns, UINT64_MAX};

    uint64_t limit = req.limit > 0 ? req.limit : 1000;
    bool     done  = false;

    auto filter_and_collect = [&](const Event& e) -> bool {
        if (done) return false;
        ++result.stats.rows_scanned;
        // Tombstones are never returned to callers
        if (e.event_type == EventType::Tombstone) return true;
        if (req.event_type && e.event_type != *req.event_type) return true;
        result.rows.push_back(e);
        ++result.stats.rows_returned;
        if (result.rows.size() >= limit) { done = true; return false; }
        return true;
    };

    // Scan memtables under lock
    {
        std::lock_guard<std::mutex> lock(mem_mu_);
        if (active_mem_ && !done)
            active_mem_->scan(start_key, end_key, filter_and_collect);
        for (auto& imm : imm_mems_) {
            if (done) break;
            imm->scan(start_key, end_key, filter_and_collect);
        }
    }

    // Snapshot the SSTable list; scan without holding the lock
    std::vector<SSTableInfo> files_to_scan;
    {
        std::lock_guard<std::mutex> lock(levels_mu_);
        for (int l = 0; l < kMaxLevels; ++l)
            for (auto& info : levels_[l])
                files_to_scan.push_back(info);
    }

    auto key_less = [](const Key& a, const Key& b) {
        return std::tie(a.symbol_id, a.timestamp_ns, a.sequence_id) <
               std::tie(b.symbol_id, b.timestamp_ns, b.sequence_id);
    };
    auto may_contain = [&](const SSTableInfo& info, const Key& s, const Key& e) {
        return !(key_less(e, info.meta.min_key) || key_less(info.meta.max_key, s));
    };

    uint64_t t_index_start = now_ns();
    for (auto& info : files_to_scan) {
        if (done) break;
        ++result.stats.files_considered;
        if (!may_contain(info, start_key, end_key)) {
            ++result.stats.files_skipped;
            continue;
        }
        uint64_t t_block_start = now_ns();
        uint64_t rows_before   = result.stats.rows_scanned;
        try {
            SSTableReader reader(info.path, block_cache_.get());
            reader.scan(start_key, end_key, filter_and_collect);
        } catch (const std::exception&) {
            // Tolerate a file that was deleted by concurrent compaction
            continue;
        }
        // Estimate blocks from rows scanned (64 KB blocks, 65-byte events → ~984/block)
        uint64_t rows_in_file = result.stats.rows_scanned - rows_before;
        result.stats.blocks_read  += std::max<uint64_t>(1, (rows_in_file + 983) / 984);
        result.stats.block_read_us += (now_ns() - t_block_start) / 1000;
    }
    result.stats.index_lookup_us = (now_ns() - t_index_start) / 1000;

    result.stats.total_latency_us = (now_ns() - t_total_start) / 1000;
    result.ok = true;

    global_metrics().query_latency_ms.record(result.stats.total_latency_us / 1000);
    return result;
}

uint64_t DB::event_count() const {
    return global_metrics().events_ingested_total.load(std::memory_order_relaxed);
}

uint64_t DB::disk_usage_bytes() const {
    uint64_t total = 0;
    try {
        for (auto& entry : std::filesystem::recursive_directory_iterator(cfg_.path))
            if (entry.is_regular_file())
                total += static_cast<uint64_t>(entry.file_size());
    } catch (...) {}
    return total;
}

std::vector<std::string> DB::symbols() const {
    return symbols_ ? symbols_->names() : std::vector<std::string>{};
}

DB::ActiveMemInfo DB::memtable_info() const {
    std::lock_guard<std::mutex> lock(mem_mu_);
    double mb = active_mem_
        ? static_cast<double>(active_mem_->size_bytes()) / (1024.0 * 1024.0) : 0.0;
    return {mb, static_cast<uint32_t>(imm_mems_.size())};
}

std::vector<LevelInfo> DB::level_infos() const {
    std::lock_guard<std::mutex> lock(levels_mu_);
    std::vector<LevelInfo> infos;
    for (int l = 0; l < kMaxLevels; ++l) {
        uint64_t sz = 0;
        for (auto& f : levels_[l]) sz += f.file_size;
        infos.push_back({
            l,
            static_cast<uint32_t>(levels_[l].size()),
            sz,
            Compaction::compaction_score(l, static_cast<uint32_t>(levels_[l].size()), sz)
        });
    }
    return infos;
}

void DB::flush_now() {
    // Rotate active to immutable, then drain synchronously
    {
        std::lock_guard<std::mutex> lock(mem_mu_);
        if (active_mem_ && !active_mem_->empty()) {
            imm_mems_.push_back(std::move(active_mem_));
            active_mem_ = std::make_unique<MemTable>(cfg_.memtable_flush_bytes);
        }
    }
    while (true) {
        std::unique_ptr<MemTable> to_flush;
        {
            std::lock_guard<std::mutex> lock(mem_mu_);
            if (imm_mems_.empty()) break;
            to_flush = std::move(imm_mems_.front());
            imm_mems_.erase(imm_mems_.begin());
        }
        do_flush(std::move(to_flush), next_sstable_seq());
    }
}

void DB::compact_now() { do_compact_l0(); }

std::filesystem::path DB::level_dir(int lvl) const {
    char buf[8];
    snprintf(buf, sizeof(buf), "l%d", lvl);
    return cfg_.path / buf;
}

uint64_t DB::next_sstable_seq() {
    return sst_seq_.fetch_add(1, std::memory_order_relaxed);
}

void DB::do_flush(std::unique_ptr<MemTable> mem, uint64_t seq) {
    if (!mem || mem->empty()) return;

    auto events = mem->to_sorted_events();
    if (events.empty()) return;

    char namebuf[64];
    snprintf(namebuf, sizeof(namebuf), "sst_l0_%06llu.sst", static_cast<unsigned long long>(seq));
    auto path = level_dir(0) / namebuf;

    SSTableWriter writer(path, 0, seq, cfg_.block_target_bytes);
    for (auto& e : events) writer.add(e);
    auto info = writer.finish();

    {
        std::lock_guard<std::mutex> lock(levels_mu_);
        levels_[0].push_back(info);
    }

    global_metrics().flush_count.fetch_add(1, std::memory_order_relaxed);
    compact_cv_.notify_one();
}

void DB::flush_loop() {
    while (!stop_flush_.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lock(flush_mu_);
            flush_cv_.wait_for(lock, std::chrono::milliseconds(100));
        }

        // Drain all pending immutable memtables
        while (true) {
            std::unique_ptr<MemTable> to_flush;
            {
                std::lock_guard<std::mutex> lock(mem_mu_);
                if (imm_mems_.empty()) break;
                to_flush = std::move(imm_mems_.front());
                imm_mems_.erase(imm_mems_.begin());
            }
            do_flush(std::move(to_flush), next_sstable_seq());
        }
    }
}

bool DB::needs_l0_compact() const {
    std::lock_guard<std::mutex> lock(levels_mu_);
    return static_cast<int>(levels_[0].size()) >= Compaction::kL0CompactionTrigger;
}

void DB::do_compact_l0() {
    std::vector<SSTableInfo> l0_files;
    {
        std::lock_guard<std::mutex> lock(levels_mu_);
        if (levels_[0].empty()) return;
        l0_files = levels_[0];
    }

    CompactionStats stats{};
    auto new_files = Compaction::run(
        l0_files, level_dir(1), 1, next_sstable_seq(),
        cfg_.sstable_target_bytes, &stats);

    if (new_files.empty()) return;

    // Remove old L0 files from disk — tolerate files already deleted
    for (auto& f : l0_files)
        try { std::filesystem::remove(f.path); } catch (...) {}

    {
        std::lock_guard<std::mutex> lock(levels_mu_);
        // Remove exactly the files we compacted (another compaction may have added more)
        levels_[0].erase(
            std::remove_if(levels_[0].begin(), levels_[0].end(),
                [&](const SSTableInfo& info) {
                    for (auto& l0 : l0_files)
                        if (l0.path == info.path) return true;
                    return false;
                }),
            levels_[0].end());
        for (auto& f : new_files) levels_[1].push_back(f);
        std::sort(levels_[1].begin(), levels_[1].end(),
            [](const SSTableInfo& a, const SSTableInfo& b) {
                return a.meta.sequence < b.meta.sequence;
            });
    }

    auto& m = global_metrics();
    m.compaction_count.fetch_add(1, std::memory_order_relaxed);
    if (stats.output_bytes > 0)
        m.write_amp_x100.store(
            static_cast<uint32_t>(stats.write_amplification * 100),
            std::memory_order_relaxed);
}

void DB::compact_loop() {
    while (!stop_compact_.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lock(compact_mu_);
            compact_cv_.wait_for(lock, std::chrono::seconds(5));
        }
        if (!stop_compact_.load(std::memory_order_acquire) && needs_l0_compact())
            do_compact_l0();
    }
}

} // namespace kronos

#include "kronosdb/db/compaction.hpp"
#include <algorithm>
#include <chrono>

namespace kronos {

std::vector<SSTableInfo> Compaction::run(
    const std::vector<SSTableInfo>& inputs,
    std::filesystem::path           out_dir,
    uint32_t                        target_level,
    uint64_t                        sequence,
    size_t                          target_file_bytes,
    CompactionStats*                stats) {

    auto t0 = std::chrono::steady_clock::now();

    if (inputs.empty()) return {};

    std::filesystem::create_directories(out_dir);

    // ── Phase 1: Load all input events into memory ──────────────────────────
    //
    // NOTE: This loads everything into RAM before sorting.  For large inputs
    // (e.g. 8 × 64 MB L0 files = 512 MB) this can be expensive.  The planned
    // replacement is a streaming k-way heap merge that stays O(B) in memory
    // where B is the block size.  Correctness first.

    struct EventWithSeq {
        Event    e;
        uint64_t sst_seq;  // used to pick newest version when keys collide
    };

    std::vector<EventWithSeq> all_events;
    uint64_t input_bytes = 0;
    uint64_t records_in  = 0;  // raw count before dedup

    for (auto& info : inputs) {
        SSTableReader reader(info.path);
        reader.scan(
            Key{0, 0, 0},
            Key{UINT32_MAX, UINT64_MAX, UINT64_MAX},
            [&](const Event& e) {
                all_events.push_back({e, info.meta.sequence});
                ++records_in;
                return true;
            });
        input_bytes += info.file_size;
    }

    // ── Phase 2: Sort and deduplicate ────────────────────────────────────────
    //
    // Sort by key ascending; for equal keys sort sst_seq descending so the
    // newest SSTable's version appears first and wins during dedup.

    std::stable_sort(all_events.begin(), all_events.end(),
        [](const EventWithSeq& a, const EventWithSeq& b) {
            if (a.e.key() != b.e.key()) return a.e.key() < b.e.key();
            return a.sst_seq > b.sst_seq;
        });

    {
        std::vector<EventWithSeq> deduped;
        deduped.reserve(all_events.size());
        for (size_t i = 0; i < all_events.size(); ++i) {
            if (i == 0 || all_events[i].e.key() != all_events[i-1].e.key())
                deduped.push_back(std::move(all_events[i]));
        }
        all_events = std::move(deduped);
    }

    // GC tombstones when compacting into the bottom level — no older copies
    // can exist below it, so the tombstone has served its purpose.
    const bool is_bottom_level = (target_level >= kMaxLevels - 1);
    if (is_bottom_level) {
        all_events.erase(
            std::remove_if(all_events.begin(), all_events.end(),
                [](const EventWithSeq& ew) {
                    return ew.e.event_type == EventType::Tombstone;
                }),
            all_events.end());
    }

    // ── Phase 3: Write output SSTables ───────────────────────────────────────

    std::vector<SSTableInfo> outputs;
    std::unique_ptr<SSTableWriter> writer;
    uint64_t out_seq       = sequence;
    uint64_t out_bytes     = 0;
    uint64_t records_out   = 0;   // records written to current output file

    auto make_path = [&]() {
        char buf[64];
        snprintf(buf, sizeof(buf), "sst_%06llu_%06llu.sst",
                 static_cast<unsigned long long>(target_level),
                 static_cast<unsigned long long>(out_seq));
        return out_dir / buf;
    };

    for (auto& ew : all_events) {
        if (!writer) {
            writer = std::make_unique<SSTableWriter>(make_path(), target_level, out_seq);
            ++out_seq;
        }
        writer->add(ew.e);
        ++records_out;

        // Rotate output file when estimated size crosses target.
        // Check every 1000 records to amortize the division.
        if (records_out % 1000 == 0) {
            // 65 bytes/event encoded + ~5% block/index overhead
            size_t estimated = records_out * Event::kEncodedSize * 105 / 100;
            if (estimated >= target_file_bytes) {
                outputs.push_back(writer->finish());
                out_bytes += outputs.back().file_size;
                writer.reset();
                records_out = 0;
            }
        }
    }
    if (writer) {
        outputs.push_back(writer->finish());
        out_bytes += outputs.back().file_size;
    }

    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    if (stats) {
        stats->input_files         = inputs.size();
        stats->output_files        = outputs.size();
        stats->input_bytes         = input_bytes;
        stats->output_bytes        = out_bytes;
        stats->records_in          = records_in;                             // pre-dedup
        stats->records_out         = all_events.size();                      // post-dedup
        stats->write_amplification = input_bytes > 0
            ? static_cast<double>(out_bytes) / static_cast<double>(input_bytes) : 1.0;
        stats->duration_ms         = ms;
    }

    return outputs;
}

double Compaction::compaction_score(int level, uint32_t file_count, uint64_t size_bytes) {
    if (level == 0)
        return static_cast<double>(file_count) / static_cast<double>(kL0CompactionTrigger);

    // Each level target is 10× the previous
    size_t target = kL1SizeTarget;
    for (int l = 1; l < level; ++l) target *= 10;
    return static_cast<double>(size_bytes) / static_cast<double>(target);
}

} // namespace kronos

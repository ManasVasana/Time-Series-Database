#pragma once
#include "kronosdb/table/sstable.hpp"
#include <vector>
#include <filesystem>
#include <cstdint>
#include <functional>

namespace kronos {

struct CompactionStats {
    uint64_t input_files;
    uint64_t output_files;
    uint64_t input_bytes;
    uint64_t output_bytes;
    uint64_t records_in;
    uint64_t records_out;
    double   write_amplification;
    double   duration_ms;
};

struct LevelInfo {
    int      level;
    uint32_t file_count;
    uint64_t size_bytes;
    double   compaction_score;  // > 1.0 means compaction needed
};

class Compaction {
public:
    // Compact given input files into target level, writing outputs to out_dir
    // Returns info for newly created SSTables
    static std::vector<SSTableInfo> run(
        const std::vector<SSTableInfo>& inputs,
        std::filesystem::path           out_dir,
        uint32_t                        target_level,
        uint64_t                        sequence,
        size_t                          target_file_bytes = 64 * 1024 * 1024,
        CompactionStats*                stats = nullptr);

    // Compute compaction score for a level
    // L0: score = file_count / 8.0
    // L1+: score = level_size / level_size_target
    static double compaction_score(int level, uint32_t file_count, uint64_t size_bytes);

    static constexpr int    kL0CompactionTrigger = 8;
    static constexpr size_t kL1SizeTarget        = 256ULL * 1024 * 1024;  // 256 MB
    static constexpr int    kMaxLevels           = 4;
};

} // namespace kronos

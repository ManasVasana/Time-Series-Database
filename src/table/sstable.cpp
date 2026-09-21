#include "kronosdb/table/sstable.hpp"
#include "kronosdb/util/block_cache.hpp"
#include "kronosdb/util/crc32c.hpp"
#include "kronosdb/util/metrics.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cassert>
#include <cstring>
#include <stdexcept>

namespace kronos {

// ── little-endian I/O helpers ─────────────────────────────────────────────────

static void w32(uint8_t* p, uint32_t v) {
    p[0]=(v)&0xFF; p[1]=(v>>8)&0xFF; p[2]=(v>>16)&0xFF; p[3]=(v>>24)&0xFF;
}
static void w64(uint8_t* p, uint64_t v) {
    for (int i=0;i<8;++i) p[i]=static_cast<uint8_t>(v>>(8*i));
}
static uint32_t r32(const uint8_t* p) {
    return (uint32_t)p[0]|((uint32_t)p[1]<<8)|((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24);
}
static uint64_t r64(const uint8_t* p) {
    uint64_t v=0; for(int i=0;i<8;++i) v|=(uint64_t)p[i]<<(8*i); return v;
}

// ── meta block encode / decode ────────────────────────────────────────────────
// event_count(8) data_bytes(8) min_key(20) max_key(20) level(4) sequence(8) = 68

static std::vector<uint8_t> encode_meta(const SSTableMeta& m) {
    std::vector<uint8_t> buf(68);
    uint8_t* p = buf.data();
    w64(p, m.event_count);                           p += 8;
    w64(p, m.data_bytes);                            p += 8;
    auto mn = m.min_key.encode(), mx = m.max_key.encode();
    std::memcpy(p, mn.data(), 20);                   p += 20;
    std::memcpy(p, mx.data(), 20);                   p += 20;
    w32(p, m.level);                                 p += 4;
    w64(p, m.sequence);
    return buf;
}

static SSTableMeta decode_meta(std::span<const uint8_t> d) {
    if (d.size() < 68) throw std::runtime_error("meta block truncated");
    const uint8_t* p = d.data();
    SSTableMeta m;
    m.event_count = r64(p);                                          p += 8;
    m.data_bytes  = r64(p);                                          p += 8;
    m.min_key     = Key::decode(std::span<const uint8_t,20>(p, 20)); p += 20;
    m.max_key     = Key::decode(std::span<const uint8_t,20>(p, 20)); p += 20;
    m.level       = r32(p);                                          p += 4;
    m.sequence    = r64(p);
    return m;
}

// ── SSTableWriter ─────────────────────────────────────────────────────────────

SSTableWriter::SSTableWriter(std::filesystem::path path, uint32_t level,
                             uint64_t sequence, size_t block_target_bytes,
                             uint32_t expected_items, double bloom_fp_rate)
    : path_(std::move(path)), level_(level), sequence_(sequence),
      builder_(block_target_bytes),
      filter_(expected_items > 0 ? expected_items : 65536, bloom_fp_rate) {
    std::filesystem::create_directories(path_.parent_path());
    fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) throw std::runtime_error("cannot create SSTable: " + path_.string());
}

SSTableWriter::~SSTableWriter() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
        try { std::filesystem::remove(path_); } catch (...) {}
    }
}

void SSTableWriter::write_bytes(const void* data, size_t n) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    size_t remaining = n;
    while (remaining > 0) {
        ssize_t w = ::write(fd_, p, remaining);
        if (w <= 0) throw std::runtime_error("SSTable write error");
        p         += static_cast<size_t>(w);
        remaining -= static_cast<size_t>(w);
    }
    write_offset_ += n;
}

void SSTableWriter::flush_block() {
    if (builder_.empty()) return;
    Key      first_key  = builder_.events().front().key();
    auto     block_data = builder_.finish();
    uint64_t offset     = write_offset_;
    write_bytes(block_data.data(), block_data.size());
    index_.add(first_key, offset, static_cast<uint32_t>(block_data.size()));
    builder_.reset();
}

void SSTableWriter::add(const Event& e) {
    if (!has_any_) {
        min_key_ = e.key();
        has_any_ = true;
    }
    max_key_ = e.key();
    ++event_count_;

    // Add key bytes to bloom filter
    auto kenc = e.key().encode();
    filter_.add(std::span<const uint8_t>(kenc.data(), kenc.size()));

    if (!builder_.add(e)) {
        flush_block();
        bool ok = builder_.add(e);
        assert(ok && "event too large for block");
        (void)ok;
    }
}

SSTableInfo SSTableWriter::finish() {
    flush_block();

    // Filter block
    auto     filter_data = filter_.encode();
    uint64_t filter_off  = write_offset_;
    uint64_t filter_size = filter_data.size();
    write_bytes(filter_data.data(), filter_data.size());

    // Index block
    auto     idx_data = index_.encode();
    uint64_t idx_off  = write_offset_;
    uint64_t idx_size = idx_data.size();
    write_bytes(idx_data.data(), idx_data.size());

    // Meta block
    SSTableMeta meta;
    meta.event_count = event_count_;
    meta.data_bytes  = filter_off;   // bytes before filter block = data blocks
    meta.min_key     = min_key_;
    meta.max_key     = max_key_;
    meta.level       = level_;
    meta.sequence    = sequence_;
    auto     meta_data = encode_meta(meta);
    uint64_t meta_off  = write_offset_;
    uint64_t meta_size = meta_data.size();
    write_bytes(meta_data.data(), meta_data.size());

    // Footer: 6×uint64 fields + magic(4) + version(4) = 56 bytes
    uint8_t footer[kSSTableFooterSize]{};
    w64(footer,      filter_off);
    w64(footer +  8, filter_size);
    w64(footer + 16, idx_off);
    w64(footer + 24, idx_size);
    w64(footer + 32, meta_off);
    w64(footer + 40, meta_size);
    w32(footer + 48, kSSTableMagic);
    w32(footer + 52, kSSTableVersion);
    write_bytes(footer, kSSTableFooterSize);

    ::fsync(fd_);
    ::close(fd_);
    fd_ = -1;

    struct stat st{};
    ::stat(path_.c_str(), &st);
    return {path_, meta, static_cast<uint64_t>(st.st_size)};
}

// ── SSTableReader ─────────────────────────────────────────────────────────────

SSTableReader::SSTableReader(std::filesystem::path path, BlockCache* cache)
    : path_(std::move(path)), cache_(cache) {
    fd_ = ::open(path_.c_str(), O_RDONLY);
    if (fd_ < 0) throw std::runtime_error("cannot open SSTable: " + path_.string());
    load_footer_and_index();
}

SSTableReader::~SSTableReader() {
    if (fd_ >= 0) ::close(fd_);
}

void SSTableReader::load_footer_and_index() {
    struct stat st{};
    ::fstat(fd_, &st);
    uint64_t file_size = static_cast<uint64_t>(st.st_size);

    if (file_size < kSSTableFooterSize)
        throw std::runtime_error("SSTable too small: " + path_.string());

    // Footer is exactly kSSTableFooterSize (56) bytes at the end of the file.
    uint8_t footer[kSSTableFooterSize]{};
    ssize_t n = ::pread(fd_, footer, kSSTableFooterSize,
                        static_cast<off_t>(file_size - kSSTableFooterSize));
    if (n != static_cast<ssize_t>(kSSTableFooterSize))
        throw std::runtime_error("SSTable footer read failed: " + path_.string());

    uint32_t magic   = r32(footer + 48);
    uint32_t version = r32(footer + 52);
    if (magic != kSSTableMagic)
        throw std::runtime_error("SSTable bad magic: " + path_.string());
    (void)version;

    uint64_t filter_off  = r64(footer);
    uint64_t filter_size = r64(footer +  8);
    uint64_t idx_off     = r64(footer + 16);
    uint64_t idx_size    = r64(footer + 24);
    uint64_t meta_off    = r64(footer + 32);
    uint64_t meta_size   = r64(footer + 40);

    // Sanity check offsets
    if (filter_off + filter_size > file_size ||
        idx_off    + idx_size    > file_size ||
        meta_off   + meta_size   > file_size)
        throw std::runtime_error("SSTable offsets out of bounds: " + path_.string());

    // Load filter block
    if (filter_size > 0) {
        std::vector<uint8_t> fbuf(filter_size);
        n = ::pread(fd_, fbuf.data(), filter_size, static_cast<off_t>(filter_off));
        if (n != static_cast<ssize_t>(filter_size))
            throw std::runtime_error("SSTable filter read failed: " + path_.string());
        filter_ = BloomFilter::decode(std::span<const uint8_t>(fbuf));
    }

    // Load index block
    std::vector<uint8_t> idx_buf(idx_size);
    n = ::pread(fd_, idx_buf.data(), idx_size, static_cast<off_t>(idx_off));
    if (n != static_cast<ssize_t>(idx_size))
        throw std::runtime_error("SSTable index read failed: " + path_.string());
    index_ = IndexBlock::decode(std::span<const uint8_t>(idx_buf));

    // Load meta block
    std::vector<uint8_t> meta_buf(meta_size);
    n = ::pread(fd_, meta_buf.data(), meta_size, static_cast<off_t>(meta_off));
    if (n != static_cast<ssize_t>(meta_size))
        throw std::runtime_error("SSTable meta read failed: " + path_.string());

    info_ = {path_, decode_meta(std::span<const uint8_t>(meta_buf)), file_size};
}

Block SSTableReader::read_block_at(uint64_t offset, uint32_t size) const {
    // Check block cache first
    if (cache_) {
        auto* cached = cache_->get(path_.string(), offset);
        if (cached) {
            global_metrics().block_cache_hits.fetch_add(1, std::memory_order_relaxed);
            global_metrics().block_cache_lookups.fetch_add(1, std::memory_order_relaxed);
            return Block::decode(std::span<const uint8_t>(*cached));
        }
        global_metrics().block_cache_lookups.fetch_add(1, std::memory_order_relaxed);
    }

    std::vector<uint8_t> buf(size);
    ssize_t n = ::pread(fd_, buf.data(), size, static_cast<off_t>(offset));
    if (n != static_cast<ssize_t>(size))
        throw std::runtime_error("SSTable block read failed: " + path_.string());

    if (cache_) cache_->put(path_.string(), offset, buf);
    return Block::decode(std::span<const uint8_t>(buf));
}

bool SSTableReader::may_contain(const Key& start, const Key& end) const noexcept {
    // Coarse key-range check first
    if (end < info_.meta.min_key || info_.meta.max_key < start) return false;
    // For point lookups or narrow ranges, use bloom filter
    if (start == end && !filter_.empty()) {
        auto kenc = start.encode();
        return filter_.may_contain(std::span<const uint8_t>(kenc.data(), kenc.size()));
    }
    return true;
}

bool SSTableReader::get(const Key& key, Event& out) const {
    // Bloom filter check
    if (!filter_.empty()) {
        auto kenc = key.encode();
        if (!filter_.may_contain(std::span<const uint8_t>(kenc.data(), kenc.size())))
            return false;
    }

    bool found = false;
    scan(key, key, [&](const Event& e) {
        out   = e;
        found = true;
        return false;
    });
    return found;
}

void SSTableReader::scan(const Key& start, const Key& end,
                          const std::function<bool(const Event&)>& cb) const {
    if (!may_contain(start, end)) return;

    const auto& entries    = index_.entries();
    auto        block_idxs = index_.blocks_in_range(start, end);

    for (size_t bi : block_idxs) {
        if (bi >= entries.size()) break;
        auto& entry = entries[bi];
        Block blk   = read_block_at(entry.block_offset, entry.block_size);
        bool  cont  = true;
        blk.scan(start, end, [&](const Event& e) {
            cont = cb(e);
            return cont;
        });
        if (!cont) return;
    }
}

} // namespace kronos

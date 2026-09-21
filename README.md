# KronosDB

A purpose-built C++23 LSM-Tree storage engine for financial time-series data. Designed to ingest market events at high throughput with nanosecond timestamp resolution, durable WAL-backed writes, and symbol-keyed range scans. Ships with a local REST server so any language can read and write through a stable HTTP contract.

## Summary

KronosDB implements a Log-Structured Merge-Tree optimized for append-heavy market data workloads. The write path is a WAL-backed arena skip list that accepts events sequentially and flushes to immutable SSTables under a leveled compaction strategy. The read path uses persisted bloom filters and an LRU block cache to avoid unnecessary disk I/O before descending into block-level binary search. The server exposes all engine internals through a documented OpenAPI contract so a frontend or another service can connect without coupling to storage details.

The design is deliberately narrow: fixed event schema, no SQL, no distribution, no authentication. The tradeoff is that everything the engine does, it does correctly and measurably.



## System Architecture

```
Write Path
  DB::append(symbol, timestamp_ns, event_type, ...)
    SymbolTable::intern()         "AAPL" -> uint32_t
    WALWriter::append()           magic(4) ver(1) len(4) crc32c(4) payload(65)  = 78 bytes/record
    SkipList::insert()            arena bump-alloc, acquire/release atomic links

  Background Flush Thread
    MemTable -> SSTableWriter     data blocks + filter block + index block + footer
    Levels[0] += new SSTable
    compact_cv_.notify_one()

  Background Compaction Thread
    L0 count >= 8 -> sort-merge   dedup by key, tombstone GC at Lmax
    Levels[1] += output SSTables
    Levels[0] -= compacted files

Read Path
  DB::query(symbol, start_ns, end_ns, event_type, limit)
    MemTable::scan()              lock-free skip list forward iteration
    foreach SSTable in Levels[]:
      key_range_check()           end < meta.min_key || meta.max_key < start -> skip
      BloomFilter::may_contain()  h1(key) + i*h2(key) over m bits -> miss -> skip
      BlockCache::get()           LRU hit -> decode cached bytes
      pread() + Block::decode()   CRC32C verify, binary search within block
      filter tombstones           EventType::Tombstone never returned to caller
```

```
┌─────────────────────────────────────────────────────────┐
│                      REST Server                        │
│              127.0.0.1:7420  (httplib)                  │
└───────────┬────────────┬────────────────────────────────┘
            │ append     │ query
            ▼            ▼
┌───────────────────────────────────────────────────────┐
│                       DB                              │
│   SymbolTable    WALWriter    BlockCache (LRU 64MB)   │
└───────────┬───────────────────────────────────────────┘
            │ insert
            ▼
┌─────────────────────┐   flush   ┌─────────────────┐
│     MemTable        │ ────────► │   Level 0       │
│  arena skip list    │           │  SSTables       │
│  lock-free reads    │           └────────┬────────┘
└─────────────────────┘                    │ compact (>= 8 files)
                                           ▼
                                  ┌─────────────────┐
                                  │   Level 1       │
                                  │  SSTables       │
                                  └────────┬────────┘
                                           │ compact
                                           ▼
                                  ┌─────────────────┐
                                  │ Level 2 / L3    │
                                  │ tombstone GC    │
                                  └─────────────────┘
```



## Data Model

All records share a fixed binary schema. The primary key is `(symbol_id, timestamp_ns, sequence_id)` encoded big-endian for byte-lexicographic ordering. The symbol string is resolved to a uint32 through a persistent symbol table on every write.

| Field | Type | Bytes | Description |
|---|---|---|---|
| timestamp_ns | uint64 | 8 | Nanoseconds since Unix epoch |
| symbol_id | uint32 | 4 | Internal ID resolved from symbol string |
| sequence_id | uint64 | 8 | Monotonic counter assigned by the engine |
| event_type | uint8 | 1 | trade=0, quote=1, add=2, modify=3, cancel=4, tombstone=0xFF |
| price_ticks | int64 | 8 | Last price in integer ticks |
| bid_px_ticks | int64 | 8 | Bid price in ticks |
| ask_px_ticks | int64 | 8 | Ask price in ticks |
| quantity | uint32 | 4 | Trade quantity |
| bid_qty | uint32 | 4 | Bid quantity |
| ask_qty | uint32 | 4 | Ask quantity |
| order_id | uint64 | 8 | Order reference |

Total: 65 bytes per event, all little-endian. No padding.



## Storage Format

### Write-Ahead Log

Each WAL record is 78 bytes:

```
Offset  Size  Field
0       4     magic = 0x4B524F4E  ('KRON', little-endian)
4       1     version = 0x01
5       4     payload_len (always 65 for current schema)
9       4     CRC32C over (version || payload_len || payload)
13      65    encoded Event
```

CRC uses the Castagnoli polynomial (0x82F63B78). Chaining is correct: `crc32c(buf2, n, prev_result)` restores the running internal state because the function XORs seed with 0xFFFFFFFF on entry. Segments rotate at a configurable size (default 128 MB). On recovery, the reader walks segments in numeric order, stops at the first bad magic or CRC, truncates the corrupt tail, and seals before opening a new segment for writes.

### SSTable (v2)

```
Offset              Content
0                   [data blocks...]        64KB target per block
filter_offset       [filter block]          BloomFilter: k(4LE) m(4LE) bits(ceil(m/8))
index_offset        [index block]           N x (first_key:20, block_offset:8, block_size:4)
meta_offset         [meta block]            event_count(8) data_bytes(8) min_key(20) max_key(20) level(4) sequence(8)
file_size - 56      [footer: 56 bytes]      filter_offset(8) filter_size(8) index_offset(8) index_size(8) meta_offset(8) meta_size(8) magic(4) version(4)
```

Each data block ends with CRC32C over all preceding bytes in the block. Magic is `0x4B535354` ('KSST'). Version 2 adds the filter block offset/size to the footer; version 1 files (pre-bloom) are rejected on open.

### Bloom Filter

Standard double-hashing construction: `h_i(x) = (h1(x) + i * h2(x)) mod m`. h1 is FNV-1a, h2 is a Murmur-style finalizer mix. Optimal parameters computed from the standard formula:

```
m = ceil(-n * ln(p) / ln(2)^2)   bit array size
k = round(m * ln(2) / n)          hash function count
```

At the default 1% false-positive rate and 64KB block size (~984 events/block), k=7 and m=9.6 bits/element. The filter is written into the SSTable during `SSTableWriter::finish()` and loaded once on `SSTableReader` construction. It is never rebuilt from events after that point.

### Key Encoding

The 20-byte on-disk key is big-endian to make byte-lexicographic comparison match semantic ordering:

```
Bytes 0-3    symbol_id   (4 bytes, big-endian)
Bytes 4-11   timestamp_ns (8 bytes, big-endian)
Bytes 12-19  sequence_id  (8 bytes, big-endian)
```

In-memory comparisons use the `Key::operator<=>` on the struct directly (compiler generates correct field-by-field comparison).

### MemTable: Arena Skip List

The active memtable is a skip list backed by a bump-pointer arena. Nodes are allocated from sequential 4KB blocks rather than per-node malloc calls. Write path holds a `std::mutex` to serialize concurrent inserts. Read path (scan, to_sorted_events) uses only `std::memory_order_acquire` loads through atomic next-pointer chains with no mutex. When the memtable is pushed to the immutable queue, the entire arena is freed in O(1) after the flush thread writes it to an SSTable.

Node layout (variable height):

```
const Key key               (20 bytes)
atomic<const Event*> val    (8 bytes, points into arena)
const int height            (4 bytes)
atomic<Node*> next_[height] (8 * height bytes)
```

Average height is 1.33 levels at branch factor 4. Expected memory per node is around 100 bytes including the value copy in the arena.



## Concurrency Model

| Component | Read | Write |
|---|---|---|
| MemTable | Lock-free (acquire loads) | `std::mutex` (single writer) |
| MemTable queue (imm_mems_) | `mem_mu_` shared | `mem_mu_` exclusive |
| SSTable level list | Snapshot under `levels_mu_` | `levels_mu_` exclusive |
| WAL | N/A | Per-writer mutex in WALWriter |
| BlockCache | `std::mutex` | `std::mutex` |
| DB::closed_ | `memory_order_acquire` | `compare_exchange_strong(acq_rel)` |
| DB::append / DB::remove | N/A | Concurrent safe |

`DB::close()` uses a compare-exchange on `closed_` so the destructor and an explicit `close()` call can race without double-shutdown. Background flush and compaction threads are joined before the final memtable drain.



## API

Full contract: [`api/v1.yaml`](api/v1.yaml). All responses include CORS headers. Errors follow `{"ok": false, "error": {"code": "...", "message": "..."}}`.

| Method | Path | Description |
|---|---|---|
| GET | `/v1/status` | Engine version, uptime, event count, disk usage |
| GET | `/v1/metrics` | p50/p90/p99/p999 append latency, throughput EMA, write/read/space amplification |
| GET | `/v1/storage/levels` | Memtable size, immutable count, per-level file count and compaction score |
| POST | `/v1/ingest/event` | Append one event, returns sequence_id and append_latency_us |
| POST | `/v1/events/delete` | Write tombstone for (symbol, timestamp_ns, sequence_id) |
| POST | `/v1/ingest/synthetic/start` | Background event generation at configurable rate and volatility |
| POST | `/v1/ingest/synthetic/stop` | Stop running job, returns events_generated |
| POST | `/v1/query` | Range scan with execution stats breakdown |
| POST | `/v1/benchmark/run` | Inline ingest benchmark, writes result JSON to disk |
| POST | `/v1/recovery/demo` | Write 100K events, corrupt tail, recover, report stats |
| GET | `/v1/live` | Server-Sent Events, 500ms cadence, streams live metrics to browser |

Query responses include a `stats` block with individual timing phases:

```json
{
  "stats": {
    "total_latency_us": 18600,
    "planning_us": 210,
    "index_lookup_us": 440,
    "block_read_us": 4910,
    "files_considered": 128,
    "files_skipped": 113,
    "blocks_read": 184,
    "rows_scanned": 1240000,
    "rows_returned": 1000
  }
}
```

`files_skipped` is the number of SSTables rejected by key-range check or bloom filter before any pread. `block_read_us` drops on repeated queries as the LRU cache warms.



## Performance

Numbers from Google Benchmark on Apple M-series, Release build (`-O3 -march=native`), NVMe SSD. Each benchmark runs through the real engine path with no mocking.

### Ingest throughput

| Scenario | Throughput | Latency (avg) |
|---|---|---|
| Single symbol, WAL enabled | 230K events/s | 4.4 us |
| Single symbol, WAL disabled (bulk load) | 2.4M events/s | 0.41 us |
| 100 symbols, WAL disabled | 1.1M events/s | 0.89 us |

WAL-enabled path includes a `write()` syscall per record plus CRC32C computation. The no-WAL path is skip list insert only and shows the raw memtable ceiling.

### WAL

| Operation | Throughput | Notes |
|---|---|---|
| WAL append (single record) | 274K records/s | 3.7 us per record including CRC32C |
| WAL sequential read (100K records) | 523K records/s | pread + CRC verify |
| Recovery 100K records | 475K records/s | WAL replay into memtable |
| Recovery 1M records | 421K records/s | Includes memtable skip list inserts |

1M record recovery completes in ~2.4s. WAL segment files are typically read in a single pass with no seeking.

### SSTable scan

| Scenario | Throughput | Notes |
|---|---|---|
| Sequential pread scan (500K events) | 1.1M events/s | 90ms for 500K events |
| Sequential mmap scan (500K events) | 6.3x faster than pread | 14ms, OS page cache does sequential prefetch |

mmap delivers a 6x scan speedup for sequential access patterns because the OS handles readahead automatically. Random access (point gets, narrow range scans) narrows this gap because pread only reads what is needed.

### Theoretical limits at sustained load

These are back-of-envelope projections based on the benchmark numbers above:

- At 230K events/s with WAL, a 64MB memtable fills in ~15 minutes and triggers a flush
- A flush of 64MB at ~1.1M events/s SSTable write throughput completes in under a second
- L0 compaction of 8 x 64MB = 512MB completes in roughly 30-60s depending on dedup ratio
- Bloom filter check at 1% FPR eliminates ~99% of irrelevant SSTable reads before any pread

All numbers above are from `bench/bench_ingest.cpp`, `bench/bench_recovery.cpp`, and `bench/bench_io_backend.cpp`. Run on your own hardware:

```bash
make bench
```

## Complexity

| Operation | Complexity | Notes |
|---|---|---|
| Append | O(log n) amortized | Skip list insert + WAL write |
| MemTable scan | O(k + log n) | k = results, lock-free forward iteration |
| SSTable point get | O(log B) | B = blocks per file, bloom filter precheck |
| SSTable range scan | O(k + log B) | Skip non-overlapping files with key range + filter |
| Memtable flush | O(n log n) | Sort already done by skip list structure |
| L0 compaction | O(n log n) | Full sort-merge of all L0 + overlapping L1 |
| Tombstone GC | O(1) | Filter pass during compaction at Lmax |
| Arena free on flush | O(1) | Delete block vector, no per-node destructors |

Write amplification is bounded by the number of levels times the compaction fanout. Space amplification during L0 accumulation is at most `L0_trigger_count` before compaction reduces it back toward 1.0x. Both are tracked and exposed at `/v1/metrics`.



## Configuration

```bash
./build/kronosdb-server \
  --db ./data \
  --port 7420 \
  --host 127.0.0.1
```

`DBConfig` parameters (defined in `include/kronosdb/db.hpp`):

| Parameter | Default | Description |
|---|---|---|
| `memtable_flush_bytes` | 64 MB | Rotate active memtable to immutable queue at this size |
| `wal_segment_bytes` | 128 MB | Create new WAL segment file when current exceeds this |
| `block_target_bytes` | 64 KB | Target size for a single SSTable data block |
| `sstable_target_bytes` | 64 MB | Rotate output SSTable file during compaction |
| `block_cache_bytes` | 64 MB | LRU block cache capacity |
| `bloom_fp_rate` | 0.01 | Target false-positive rate for bloom filter construction |
| `background_compaction` | true | Disable for tests to control flush and compaction explicitly |



## Build

Requires CMake >= 3.25, Ninja, and a compiler with C++23 support (Apple Clang 15+, GCC 13+, or Clang 16+). All dependencies are fetched by CMake FetchContent on first configure.

```bash
git clone https://github.com/heyman7913/kronos-db
cd kronos-db
make build          # cmake configure + ninja
make test           # run 40-test suite
make bench          # all four benchmark binaries
make fmt            # clang-format all source
```

Or directly:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -G Ninja -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build -- -j$(nproc)
cd build && ctest --output-on-failure
```

Dependencies pulled by FetchContent:

| Library | Version | Use |
|---|---|---|
| nlohmann/json | 3.11.3 | JSON serialization in server |
| cpp-httplib | 0.18.3 | HTTP server, SSE |
| GoogleTest | 1.14.0 | Test suite |
| Google Benchmark | 1.8.5 | Benchmark binaries |



## Source Layout

```
include/kronosdb/
  key.hpp               20-byte primary key with big-endian encode/decode
  event.hpp             65-byte fixed event schema, EventType enum
  arena.hpp             Bump-pointer allocator, O(1) bulk free
  skiplist.hpp          LevelDB-style lock-free read skip list over arena
  memtable.hpp          Arena skip list + min/max tracking + flush threshold
  wal.hpp               WALWriter (append + seal + truncate), WALReader (CRC recovery)
  bloom_filter.hpp      Double-hash bloom filter, serializable, persisted in SSTable
  block.hpp             BlockBuilder (fill to target size), Block (decode + scan)
  index.hpp             Sparse block index, binary search for range overlap
  sstable.hpp           SSTableWriter (v2 with filter block), SSTableReader + point get
  block_cache.hpp       LRU block cache keyed by (path, offset)
  compaction.hpp        sort-merge run, tombstone GC at Lmax, compaction score
  db.hpp                DB class, DBConfig, AppendResult
  recovery.hpp          WAL replay into MemTable, RecoveryResult
  metrics.hpp           Power-of-2 latency histogram, EMA throughput tracker
  query.hpp             QueryRequest, QueryStats, QueryResult
  server.hpp            httplib-backed REST server, SyntheticGenerator ownership
  synthetic.hpp         Background event generator with volatility regimes
  symbol_table.hpp      Append-only string->uint32 map persisted to disk
  crc32c.hpp            Software CRC32C, thread-safe IIFE table init

src/                    One .cpp per header
bench/                  bench_ingest, bench_scan, bench_recovery, bench_io_backend
test/                   40 tests across WAL, SSTable, compaction, query, synthetic
tools/                  kronosdb-cli, generate_synthetic
api/v1.yaml             OpenAPI 3.1 contract
```



## WAL Crash Recovery

On startup, KronosDB opens every `wal_*.log` segment in numeric order. For each record it verifies the magic bytes, checks that the declared payload length fits within the remaining file, and validates the CRC32C. At the first failure it stops reading, records the byte offset of the last valid record, and calls `WALWriter::truncate_and_seal(clean_end_offset)` before accepting new writes. This prevents new records from landing past a corrupt tail on the next startup.

No committed record is lost. A write is only acknowledged after `WALWriter::append()` returns, which only returns after the kernel has accepted the write. Any partial write is detected by the torn-tail check.

```bash
# Trigger the demo:
curl -X POST http://127.0.0.1:7420/v1/recovery/demo
```

```json
{
  "records_before_crash": 100000,
  "records_recovered": 99995,
  "torn_record_detected": true,
  "corrupted_tail_bytes": 384,
  "recovery_time_ms": 1310,
  "committed_records_lost": 0
}
```



## Synthetic Data Generation

The generator produces configurable market event mixes through the real write path (WAL + memtable). Bypassing the write path is not supported and not the point.

```bash
./build/generate_synthetic \
  --symbols  100       \
  --rate     100000    \
  --duration 30        \
  --regime   normal
```

Volatility regimes control price variance relative to a base price of 1,000,000 ticks: `normal` uses 0.1% standard deviation, `high_vol` uses 0.5%, `flash_crash` uses 2.0%.

Event mix defaults: 70% quote, 20% trade, 5% add, 3% modify, 2% cancel.



## Workbench

[kronos-workbench](https://github.com/heyman7913/kronos-workbench) is the companion browser frontend. It connects to the running server over HTTP and provides a live metrics dashboard, query panel, and event inspector. The API contract in `api/v1.yaml` is the only coupling between the two repos.



## License

MIT

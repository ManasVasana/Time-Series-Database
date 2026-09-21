cmake_minimum_required(VERSION 3.25)
project(kronosdb VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

option(KRONOS_BUILD_TESTS      "Build test suite"  ON)
option(KRONOS_BUILD_BENCHMARKS "Build benchmarks"  ON)
option(KRONOS_BUILD_TOOLS      "Build CLI tools"   ON)

add_compile_options(-Wall -Wextra -Wpedantic)
if(CMAKE_BUILD_TYPE STREQUAL "Release")
  add_compile_options(-O3 -march=native)
endif()

# ── dependencies ──────────────────────────────────────────────────────────────
include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

FetchContent_Declare(nlohmann_json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG        v3.11.3
  GIT_SHALLOW    TRUE)
FetchContent_MakeAvailable(nlohmann_json)

FetchContent_Declare(httplib_src
  GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
  GIT_TAG        v0.18.3
  GIT_SHALLOW    TRUE)
FetchContent_GetProperties(httplib_src)
if(NOT httplib_src_POPULATED)
  FetchContent_Populate(httplib_src)
endif()
add_library(httplib_iface INTERFACE)
target_include_directories(httplib_iface INTERFACE ${httplib_src_SOURCE_DIR})
add_library(httplib::httplib ALIAS httplib_iface)

if(KRONOS_BUILD_TESTS)
  FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
    GIT_SHALLOW    TRUE)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  include(GoogleTest)
  enable_testing()
endif()

if(KRONOS_BUILD_BENCHMARKS)
  FetchContent_Declare(benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG        v1.8.5
    GIT_SHALLOW    TRUE)
  set(BENCHMARK_ENABLE_TESTING  OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_INSTALL  OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(benchmark)
endif()

# ── core library ──────────────────────────────────────────────────────────────
add_library(kronosdb_core STATIC
  src/core/event.cpp
  src/util/symbol_table.cpp
  src/table/block.cpp
  src/table/index.cpp
  src/table/bloom_filter.cpp
  src/storage/wal.cpp
  src/storage/memtable.cpp
  src/table/sstable.cpp
  src/db/compaction.cpp
  src/util/metrics.cpp
  src/db/query.cpp
  src/db/recovery.cpp
  src/net/synthetic.cpp
  src/db/db.cpp
  src/net/server.cpp
  src/util/block_cache.cpp
)
target_include_directories(kronosdb_core PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_link_libraries(kronosdb_core PUBLIC
  nlohmann_json::nlohmann_json
  httplib::httplib
  pthread)
target_compile_features(kronosdb_core PUBLIC cxx_std_23)

# ── server executable ─────────────────────────────────────────────────────────
add_executable(kronosdb-server src/main_server.cpp)
target_link_libraries(kronosdb-server PRIVATE kronosdb_core)

# ── tools ─────────────────────────────────────────────────────────────────────
if(KRONOS_BUILD_TOOLS)
  add_executable(kronosdb-cli tools/kronosdb_cli.cpp)
  target_link_libraries(kronosdb-cli PRIVATE kronosdb_core)

  add_executable(generate_synthetic tools/generate_synthetic.cpp)
  target_link_libraries(generate_synthetic PRIVATE kronosdb_core)
endif()

# ── tests ─────────────────────────────────────────────────────────────────────
if(KRONOS_BUILD_TESTS)
  add_executable(kronos_tests
    test/event_encoding_test.cpp
    test/wal_recovery_test.cpp
    test/wal_corruption_test.cpp
    test/sstable_test.cpp
    test/scan_test.cpp
    test/compaction_test.cpp
    test/synthetic_test.cpp
    test/query_stats_test.cpp
    test/bloom_filter_test.cpp
    test/block_cache_test.cpp
    test/tombstone_test.cpp
    test/symbol_table_test.cpp
  )
  target_link_libraries(kronos_tests PRIVATE kronosdb_core GTest::gtest_main)
  gtest_discover_tests(kronos_tests)
endif()

# ── benchmarks ────────────────────────────────────────────────────────────────
if(KRONOS_BUILD_BENCHMARKS)
  foreach(bench bench_ingest bench_scan bench_recovery bench_io_backend)
    add_executable(${bench} bench/${bench}.cpp)
    target_link_libraries(${bench} PRIVATE kronosdb_core benchmark::benchmark_main)
  endforeach()
endif()

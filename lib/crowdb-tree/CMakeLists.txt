cmake_minimum_required(VERSION 3.20)
project(crowdb-tree LANGUAGES CXX)

# ── Standard / warnings ───────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

add_compile_options(-Wall -Wextra -Werror)

# ── Optional sanitizers: -DCROWDB_TREE_SANITIZER=address|thread|undefined ──
if(CROWDB_TREE_SANITIZER)
  message(STATUS "crowdb-tree sanitizer: ${CROWDB_TREE_SANITIZER}")
  add_compile_options(-fsanitize=${CROWDB_TREE_SANITIZER} -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=${CROWDB_TREE_SANITIZER})
endif()

# ── liburing (io_uring reactor) ────────────────────────────────────
# The reactor now lives in crowdb-common; crowdb-tree gets liburing
# transitively via the crowdbcommon PUBLIC link (CROWDB_HAVE_LIBURING
# compile definition + liburing library). We still need to probe
# liburing here to decide whether to compile block_async_page_store.cpp
# (which uses the reactor directly).
# HINTS ENV CONDA_PREFIX: prefer the pixi/conda env's liburing (see
# crowdb-common/cpp/CMakeLists.txt for rationale).
find_path(LIBURING_INCLUDE_DIR liburing.h HINTS ENV CONDA_PREFIX PATH_SUFFIXES include)
find_library(LIBURING_LIB NAMES uring HINTS ENV CONDA_PREFIX PATH_SUFFIXES lib)

# ── crowdb-common (shared utils: crc32c, log, gzip, metrics, reactor) ──
# Built in-tree via add_subdirectory so a single `cmake -S crowdb-tree`
# invocation produces both libcrowdbcommon.a and libcrowdb-tree.a. crowdb-tree
# links crowdbcommon PUBLIC because its public headers (frame_page.h,
# page_codec.h, compressor.h, crowdb-tree.h) include crowdb-common headers.
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../crowdb-common/cpp crowdb-common-build)

# ── Library ───────────────────────────────────────────────────────
file(GLOB_RECURSE CROWDB_TREE_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
if(NOT (LIBURING_INCLUDE_DIR AND LIBURING_LIB))
  list(REMOVE_ITEM CROWDB_TREE_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/src/block_async_page_store.cpp
  )
endif()
add_library(crowdb-tree STATIC ${CROWDB_TREE_SRC})
target_include_directories(crowdb-tree PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
find_package(Threads REQUIRED)
target_link_libraries(crowdb-tree PUBLIC Threads::Threads crowdbcommon)

# ── Abseil (MemTable ordered map: absl::btree_map, plan-tree #9) ───
find_package(absl REQUIRED)
target_link_libraries(crowdb-tree PUBLIC absl::btree)

# ── spdlog logging ────────────────────────────────────────────────
# spdlog, zlib, and the CROWDB_HAVE_SPDLOG gate now come from `crowdbcommon`
# (linked PUBLIC above) — the logging facade, compressing sink, gzip
# helper, and metrics core moved there (R12). crowdb-tree's remaining
# sources include crowdb-common/log.h and use CRB_LOG_*; the spdlog/fmt/zlib
# link requirement propagates transitively to final executables through
# crowdbcommon's PRIVATE link. Keep the SPDLOG_ACTIVE_LEVEL floor on
# crowdb-tree so its includers of crowdb-common/log.h get the same level
# filtering as crowdbcommon's own TUs.
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  target_compile_definitions(crowdb-tree PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)
else()
  target_compile_definitions(crowdb-tree PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO)
endif()

# ── LZ4 page compression (PT10 / PT13) ────────────────────────────
# Resolution order:
#   1. System LZ4 dev package (header + lib) discovered via CMake / pixi.
#   2. None: compression compiles out to an identity codec (still correct).
find_path(LZ4_INCLUDE_DIR lz4.h)
find_library(LZ4_LIB NAMES lz4)
if(LZ4_INCLUDE_DIR AND LZ4_LIB)
  message(STATUS "crowdb-tree LZ4: system ${LZ4_LIB} (${LZ4_INCLUDE_DIR})")
  target_include_directories(crowdb-tree SYSTEM PRIVATE ${LZ4_INCLUDE_DIR})
  target_compile_definitions(crowdb-tree PUBLIC CROWDB_TREE_HAVE_LZ4=1)
  target_link_libraries(crowdb-tree PUBLIC ${LZ4_LIB})
else()
  message(STATUS "crowdb-tree LZ4: not found (compression disabled)")
endif()

# ── Tests (GoogleTest) ────────────────────────────────────────────
enable_testing()
find_package(GTest REQUIRED)
include(GoogleTest)

file(GLOB_RECURSE CROWDB_TREE_TEST_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)
if(NOT (LIBURING_INCLUDE_DIR AND LIBURING_LIB))
  list(REMOVE_ITEM CROWDB_TREE_TEST_SRC ${CMAKE_CURRENT_SOURCE_DIR}/tests/unit/reactor_test.cpp)
endif()
add_executable(crowdb_tree_tests ${CROWDB_TREE_TEST_SRC})
target_include_directories(crowdb_tree_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
target_link_libraries(crowdb_tree_tests PRIVATE crowdb-tree GTest::gtest GTest::gtest_main)
# GCC 13 -Warray-bounds/-Wstringop-overflow false positives: buffer::alloc
# returns heap memory, but GCC thinks b.data() points into the stack object b.
# These flags are GCC-specific (clang doesn't know -Wstringop-overflow); guard
# so Apple clang / other compilers don't fail with -Werror=unknown-warning-option.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(crowdb_tree_tests PRIVATE -Wno-array-bounds -Wno-stringop-overflow)
endif()
# PRE_TEST: enumerate tests at ctest time, not build time. Required so sanitizer
# builds (which can't run the binary at build time under restrictive ASLR) build
# cleanly; the suite is then run via `setarch -R` (see Makefile).
gtest_discover_tests(crowdb_tree_tests DISCOVERY_MODE PRE_TEST)

# ── Microbenchmarks (opt-in: -DCROWDB_TREE_BENCH=ON, plan-tree #9/Q2) ──
# Compares MemTable ordered containers (std::map / absl::btree_map / folly skip
# list). Off by default so the normal build/test path needs no Google Benchmark.
option(CROWDB_TREE_BENCH "Build crowdb-tree microbenchmarks" OFF)
if(CROWDB_TREE_BENCH)
  find_package(benchmark REQUIRED)
  file(GLOB CROWDB_TREE_BENCH_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/bench/*.cpp)
  add_executable(crowdb_tree_bench ${CROWDB_TREE_BENCH_SRC})
  target_link_libraries(crowdb_tree_bench PRIVATE benchmark::benchmark absl::btree crowdb-tree)
  # Third-party headers (folly/absl/benchmark) are not -Werror clean.
  target_compile_options(crowdb_tree_bench PRIVATE -Wno-error)
  find_package(folly CONFIG QUIET)
  if(folly_FOUND)
    target_link_libraries(crowdb_tree_bench PRIVATE Folly::folly)
    target_compile_definitions(crowdb_tree_bench PRIVATE CROWDB_TREE_BENCH_FOLLY=1)
    message(STATUS "crowdb-tree bench: folly skip-list enabled")
  else()
    message(STATUS "crowdb-tree bench: folly not found; skip-list bench disabled")
  endif()

  # Standalone scan per-step profiler: own main(), no Google Benchmark
  # dependency. Isolates the 1KiB anomaly's root cause via Crowtree::scan_profile.
  add_executable(scan_step_profile bench/scan_step_bench.cpp)
  target_link_libraries(scan_step_profile PRIVATE crowdb-tree absl::btree)
endif()

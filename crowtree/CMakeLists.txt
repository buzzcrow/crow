cmake_minimum_required(VERSION 3.20)
project(crowtree LANGUAGES CXX)

# ── Standard / warnings ───────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

add_compile_options(-Wall -Wextra -Werror)

# ── Optional sanitizers: -DCROWTREE_SANITIZER=address|thread|undefined ──
if(CROWTREE_SANITIZER)
  message(STATUS "crowtree sanitizer: ${CROWTREE_SANITIZER}")
  add_compile_options(-fsanitize=${CROWTREE_SANITIZER} -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=${CROWTREE_SANITIZER})
endif()

# ── liburing (io_uring reactor, plan-tree #11 Phase 0/1) ──────────
# Linux-only kernel interface -- no macOS build exists, so Reactor /
# AsyncPageStore / BlockAsyncPageStore are excluded from the build entirely
# (not compiled to no-ops) when not found, matching
# design-crowtree-async.md §10's macOS dev-path note. Probed before the
# source glob below so the exclusion list can be applied pre-add_library.
find_path(LIBURING_INCLUDE_DIR liburing.h)
find_library(LIBURING_LIB NAMES uring)

# ── crow-common (shared utils: crc32c, log, gzip, metrics) ────────
# Built in-tree via add_subdirectory so a single `cmake -S crowtree`
# invocation produces both libcrowcommon.a and libcrowtree.a. crowtree
# links crowcommon PUBLIC because its public headers (frame_page.h,
# page_codec.h, compressor.h, crowtree.h) include crow-common headers.
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../crow-common/cpp crow-common-build)

# ── Library ───────────────────────────────────────────────────────
file(GLOB_RECURSE CROWTREE_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
if(NOT (LIBURING_INCLUDE_DIR AND LIBURING_LIB))
  list(REMOVE_ITEM CROWTREE_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/src/reactor.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/block_async_page_store.cpp
  )
endif()
add_library(crowtree STATIC ${CROWTREE_SRC})
target_include_directories(crowtree PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
find_package(Threads REQUIRED)
target_link_libraries(crowtree PUBLIC Threads::Threads crowcommon)

if(LIBURING_INCLUDE_DIR AND LIBURING_LIB)
  message(STATUS "crowtree liburing: ${LIBURING_LIB} (${LIBURING_INCLUDE_DIR})")
  target_include_directories(crowtree SYSTEM PRIVATE ${LIBURING_INCLUDE_DIR})
  target_compile_definitions(crowtree PUBLIC CROWTREE_HAVE_LIBURING=1)
  target_link_libraries(crowtree PUBLIC ${LIBURING_LIB})
else()
  message(STATUS "crowtree liburing: not found (io_uring reactor disabled)")
endif()

# ── Abseil (MemTable ordered map: absl::btree_map, plan-tree #9) ───
find_package(absl REQUIRED)
target_link_libraries(crowtree PUBLIC absl::btree)

# ── spdlog logging ────────────────────────────────────────────────
# spdlog, zlib, and the CROW_HAVE_SPDLOG gate now come from `crowcommon`
# (linked PUBLIC above) — the logging facade, compressing sink, gzip
# helper, and metrics core moved there (R12). crowtree's remaining
# sources include crow-common/log.h and use CR_LOG_*; the spdlog/fmt/zlib
# link requirement propagates transitively to final executables through
# crowcommon's PRIVATE link. Keep the SPDLOG_ACTIVE_LEVEL floor on
# crowtree so its includers of crow-common/log.h get the same level
# filtering as crowcommon's own TUs.
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  target_compile_definitions(crowtree PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)
else()
  target_compile_definitions(crowtree PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO)
endif()

# ── LZ4 page compression (PT10 / PT13) ────────────────────────────
# Resolution order:
#   1. System LZ4 dev package (header + lib) discovered via CMake / pixi.
#   2. None: compression compiles out to an identity codec (still correct).
find_path(LZ4_INCLUDE_DIR lz4.h)
find_library(LZ4_LIB NAMES lz4)
if(LZ4_INCLUDE_DIR AND LZ4_LIB)
  message(STATUS "crowtree LZ4: system ${LZ4_LIB} (${LZ4_INCLUDE_DIR})")
  target_include_directories(crowtree SYSTEM PRIVATE ${LZ4_INCLUDE_DIR})
  target_compile_definitions(crowtree PUBLIC CROWTREE_HAVE_LZ4=1)
  target_link_libraries(crowtree PUBLIC ${LZ4_LIB})
else()
  message(STATUS "crowtree LZ4: not found (compression disabled)")
endif()

# ── Tests (GoogleTest) ────────────────────────────────────────────
enable_testing()
find_package(GTest REQUIRED)
include(GoogleTest)

file(GLOB_RECURSE CROWTREE_TEST_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)
if(NOT (LIBURING_INCLUDE_DIR AND LIBURING_LIB))
  list(REMOVE_ITEM CROWTREE_TEST_SRC ${CMAKE_CURRENT_SOURCE_DIR}/tests/unit/reactor_test.cpp)
endif()
add_executable(crowtree_tests ${CROWTREE_TEST_SRC})
target_include_directories(crowtree_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
target_link_libraries(crowtree_tests PRIVATE crowtree GTest::gtest GTest::gtest_main)
# PRE_TEST: enumerate tests at ctest time, not build time. Required so sanitizer
# builds (which can't run the binary at build time under restrictive ASLR) build
# cleanly; the suite is then run via `setarch -R` (see Makefile).
gtest_discover_tests(crowtree_tests DISCOVERY_MODE PRE_TEST)

# ── Microbenchmarks (opt-in: -DCROWTREE_BENCH=ON, plan-tree #9/Q2) ──
# Compares MemTable ordered containers (std::map / absl::btree_map / folly skip
# list). Off by default so the normal build/test path needs no Google Benchmark.
option(CROWTREE_BENCH "Build crowtree microbenchmarks" OFF)
if(CROWTREE_BENCH)
  find_package(benchmark REQUIRED)
  file(GLOB CROWTREE_BENCH_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/bench/*.cpp)
  add_executable(crowtree_bench ${CROWTREE_BENCH_SRC})
  target_link_libraries(crowtree_bench PRIVATE benchmark::benchmark absl::btree)
  # Third-party headers (folly/absl/benchmark) are not -Werror clean.
  target_compile_options(crowtree_bench PRIVATE -Wno-error)
  find_package(folly CONFIG QUIET)
  if(folly_FOUND)
    target_link_libraries(crowtree_bench PRIVATE Folly::folly)
    target_compile_definitions(crowtree_bench PRIVATE CROWTREE_BENCH_FOLLY=1)
    message(STATUS "crowtree bench: folly skip-list enabled")
  else()
    message(STATUS "crowtree bench: folly not found; skip-list bench disabled")
  endif()
endif()

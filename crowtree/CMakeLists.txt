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

# ── Library ───────────────────────────────────────────────────────
file(GLOB_RECURSE CROWTREE_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(crowtree STATIC ${CROWTREE_SRC})
target_include_directories(crowtree PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
find_package(Threads REQUIRED)
target_link_libraries(crowtree PUBLIC Threads::Threads)

# ── Abseil (MemTable ordered map: absl::btree_map, plan-tree #9) ───
find_package(absl REQUIRED)
target_link_libraries(crowtree PUBLIC absl::btree)

# ── spdlog logging (plan-tree #10) ────────────────────────────────
# Gated behind CROWTREE_HAVE_SPDLOG (mirrors the LZ4 pattern): the CMake build
# gets an async rotating-file logger; the Rust FFI cc build (no spdlog) compiles
# the CT_LOG_* macros to no-ops. Linked PRIVATE — libcrowtree is STATIC, so CMake
# still propagates the spdlog/fmt link requirement to the final executables while
# keeping the header/compile-def surface off the public interface.
find_package(spdlog REQUIRED)
target_link_libraries(crowtree PRIVATE spdlog::spdlog)
target_compile_definitions(crowtree PRIVATE CROWTREE_HAVE_SPDLOG=1)
# Compile-time level floor: keep trace/debug in Debug builds, strip below info in
# optimized builds (SPDLOG_ACTIVE_LEVEL elides lower calls entirely).
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
  target_include_directories(crowtree PRIVATE ${LZ4_INCLUDE_DIR})
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
add_executable(crowtree_tests ${CROWTREE_TEST_SRC})
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

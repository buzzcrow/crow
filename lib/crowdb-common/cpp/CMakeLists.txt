cmake_minimum_required(VERSION 3.20)
project(crowdbcommon LANGUAGES CXX)

# ── Standard / warnings ───────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

add_compile_options(-Wall -Wextra -Werror)

# ── liburing (io_uring reactor, shared by crowdb-tree and diskio) ────
# Linux-only kernel interface -- no macOS build exists, so Reactor is
# excluded from the build entirely (not compiled to a no-op) when not
# found. Probed before the source glob below so the exclusion list can
# be applied pre-add_library.
# HINTS ENV CONDA_PREFIX: prefer the pixi/conda env's liburing over any stale
# system copy (e.g. /usr/local) — the env ships a newer liburing whose headers
# declare io_uring_sqe_shift / io_uring_load_sq_head used by diskio_uring.cpp.
find_path(LIBURING_INCLUDE_DIR liburing.h HINTS ENV CONDA_PREFIX PATH_SUFFIXES include)
find_library(LIBURING_LIB NAMES uring HINTS ENV CONDA_PREFIX PATH_SUFFIXES lib)

# ── Library ───────────────────────────────────────────────────────
file(GLOB_RECURSE CROWDBCOMMON_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
if(NOT (LIBURING_INCLUDE_DIR AND LIBURING_LIB))
  list(REMOVE_ITEM CROWDBCOMMON_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/src/diskio_uring.cpp
  )
endif()
add_library(crowdbcommon STATIC ${CROWDBCOMMON_SRC})
target_include_directories(crowdbcommon PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
find_package(Threads REQUIRED)
target_link_libraries(crowdbcommon PUBLIC Threads::Threads)

if(LIBURING_INCLUDE_DIR AND LIBURING_LIB)
  message(STATUS "crowdb-common liburing: ${LIBURING_LIB} (${LIBURING_INCLUDE_DIR})")
  target_include_directories(crowdbcommon SYSTEM PRIVATE ${LIBURING_INCLUDE_DIR})
  target_compile_definitions(crowdbcommon PUBLIC CROWDB_HAVE_LIBURING=1)
  target_link_libraries(crowdbcommon PUBLIC ${LIBURING_LIB})
else()
  message(STATUS "crowdb-common liburing: not found (io_uring reactor disabled)")
endif()

# ── spdlog logging ────────────────────────────────────────────────
# Gated behind CROWDB_HAVE_SPDLOG (PUBLIC so includers of log.h see the
# spdlog branch). Linked PUBLIC because crowdb-common's public headers
# (log.h, compressing_sink.h) include spdlog headers — consumers must
# inherit spdlog's INTERFACE_COMPILE_DEFINITIONS (SPDLOG_COMPILED_LIB /
# SPDLOG_SHARED_LIB / SPDLOG_FMT_EXTERNAL) so they use the dylib's
# declarations and registry rather than compiling spdlog in
# SPDLOG_HEADER_ONLY mode (which would give each TU its own inline
# registry, disjoint from crowdbcommon's set_default_logger calls).
find_package(spdlog REQUIRED)
target_link_libraries(crowdbcommon PUBLIC spdlog::spdlog)
target_compile_definitions(crowdbcommon PUBLIC CROWDB_HAVE_SPDLOG=1)

# zlib for gzip compression of rotated log files (compressing_sink).
find_package(ZLIB REQUIRED)
target_link_libraries(crowdbcommon PRIVATE ZLIB::ZLIB)

# ISA-L for SIMD-optimized CRC32C (crc32_iscsi) in crowdb-common/crc32c.h.
find_path(ISAL_INCLUDE_DIR isa-l/crc.h)
find_library(ISAL_LIB NAMES isal)
if(ISAL_INCLUDE_DIR AND ISAL_LIB)
  message(STATUS "crowdb-common ISA-L: ${ISAL_LIB} (${ISAL_INCLUDE_DIR})")
  target_include_directories(crowdbcommon PUBLIC ${ISAL_INCLUDE_DIR})
  target_link_libraries(crowdbcommon PUBLIC ${ISAL_LIB})
else()
  message(FATAL_ERROR "ISA-L not found (required for CRC32C)")
endif()

# Compile-time level floor: keep trace/debug in Debug builds, strip below info in
# optimized builds (SPDLOG_ACTIVE_LEVEL elides lower calls entirely).
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  target_compile_definitions(crowdbcommon PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)
else()
  target_compile_definitions(crowdbcommon PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO)
endif()

# ── Tests ─────────────────────────────────────────────────────────
enable_testing()
include(GoogleTest)
find_package(GTest REQUIRED)
file(GLOB_RECURSE CROWDBCOMMON_TEST_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)
if(NOT (LIBURING_INCLUDE_DIR AND LIBURING_LIB))
  list(REMOVE_ITEM CROWDBCOMMON_TEST_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/diskio_uring_test.cpp
  )
endif()
add_executable(crowdbcommon_tests ${CROWDBCOMMON_TEST_SRC})
target_include_directories(crowdbcommon_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
target_link_libraries(crowdbcommon_tests PRIVATE crowdbcommon GTest::gtest)
gtest_discover_tests(crowdbcommon_tests DISCOVERY_MODE PRE_TEST)

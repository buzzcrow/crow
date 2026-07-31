cmake_minimum_required(VERSION 3.20)
project(crowcommon LANGUAGES CXX)

# ── Standard / warnings ───────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

add_compile_options(-Wall -Wextra -Werror)

# ── Library ───────────────────────────────────────────────────────
file(GLOB_RECURSE CROWCOMMON_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(crowcommon STATIC ${CROWCOMMON_SRC})
target_include_directories(crowcommon PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
find_package(Threads REQUIRED)
target_link_libraries(crowcommon PUBLIC Threads::Threads)

# ── spdlog logging ────────────────────────────────────────────────
# Gated behind CROW_HAVE_SPDLOG (PUBLIC so includers of log.h see the
# spdlog branch). Linked PUBLIC because crow-common's public headers
# (log.h, compressing_sink.h) include spdlog headers — consumers must
# inherit spdlog's INTERFACE_COMPILE_DEFINITIONS (SPDLOG_COMPILED_LIB /
# SPDLOG_SHARED_LIB / SPDLOG_FMT_EXTERNAL) so they use the dylib's
# declarations and registry rather than compiling spdlog in
# SPDLOG_HEADER_ONLY mode (which would give each TU its own inline
# registry, disjoint from crowcommon's set_default_logger calls).
find_package(spdlog REQUIRED)
target_link_libraries(crowcommon PUBLIC spdlog::spdlog)
target_compile_definitions(crowcommon PUBLIC CROW_HAVE_SPDLOG=1)

# zlib for gzip compression of rotated log files (compressing_sink).
find_package(ZLIB REQUIRED)
target_link_libraries(crowcommon PRIVATE ZLIB::ZLIB)

# ISA-L for SIMD-optimized CRC32C (crc32_iscsi) in crow-common/crc32c.h.
find_path(ISAL_INCLUDE_DIR isa-l/crc.h)
find_library(ISAL_LIB NAMES isal)
if(ISAL_INCLUDE_DIR AND ISAL_LIB)
  message(STATUS "crow-common ISA-L: ${ISAL_LIB} (${ISAL_INCLUDE_DIR})")
  target_include_directories(crowcommon PUBLIC ${ISAL_INCLUDE_DIR})
  target_link_libraries(crowcommon PUBLIC ${ISAL_LIB})
else()
  message(FATAL_ERROR "ISA-L not found (required for CRC32C)")
endif()

# Compile-time level floor: keep trace/debug in Debug builds, strip below info in
# optimized builds (SPDLOG_ACTIVE_LEVEL elides lower calls entirely).
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  target_compile_definitions(crowcommon PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)
else()
  target_compile_definitions(crowcommon PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO)
endif()

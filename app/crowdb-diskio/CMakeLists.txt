cmake_minimum_required(VERSION 3.20)
project(crowdb-diskio LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

add_compile_options(-Wall -Wextra -Werror)

# ── crowdb-rpc (RPC server, framing, flatbuffer C++ codegen) ────────
# Built in-tree so crowdb-diskio can link RpcServer and use the generated
# flatbuffer headers (diskio_generated.h etc.) from crowdb-rpc's build dir.
# crowdb-rpc itself pulls in crowdb-common via add_subdirectory, so we don't
# add crowdb-common separately here (avoids duplicate target collision).
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../../lib/crowdb-rpc crowdb-rpc-build)

# ── crowdb-kv-client FFI (Rust cdylib with --features ffi) ──────────
# crowdb-kv-client produces both an rlib (for Rust dependents) and a
# cdylib (for C++ consumers). The cdylib exports C ABI symbols only
# when built with the `ffi` feature.
set(CROWDB_KV_CLIENT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../lib/crowdb-kv-client)
set(CROWDB_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../..)
if(CMAKE_BUILD_TYPE STREQUAL "Release")
  set(CARGO_PROFILE release)
else()
  set(CARGO_PROFILE debug)
endif()
if(APPLE)
  set(CROWDB_KV_CLIENT_FFI_LIB ${CROWDB_ROOT_DIR}/target/${CARGO_PROFILE}/libcrowdb_kv_client.dylib)
else()
  set(CROWDB_KV_CLIENT_FFI_LIB ${CROWDB_ROOT_DIR}/target/${CARGO_PROFILE}/libcrowdb_kv_client.so)
endif()
add_custom_command(
  OUTPUT ${CROWDB_KV_CLIENT_FFI_LIB}
  COMMAND pixi run cargo build -p crowdb-kv-client --features ffi
          $<$<STREQUAL:${CARGO_PROFILE},release>:--release>
  WORKING_DIRECTORY ${CROWDB_ROOT_DIR}
  COMMENT "Building crowdb-kv-client (Rust cdylib with ffi feature)"
  VERBATIM
)
add_custom_target(crowdb-kv-client-ffi DEPENDS ${CROWDB_KV_CLIENT_FFI_LIB})

# ── Source files (exclude dio_main.cpp from the library) ──────────
file(GLOB_RECURSE CROWDB_DISKIO_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
list(REMOVE_ITEM CROWDB_DISKIO_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/dio_main.cpp)

add_library(crowdb-diskio-lib STATIC ${CROWDB_DISKIO_SRC})
target_include_directories(crowdb-diskio-lib
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src
         ${CMAKE_CURRENT_SOURCE_DIR}/../../lib/crowdb-common/cpp/include
         ${CMAKE_CURRENT_SOURCE_DIR}/../../lib/crowdb-rpc/include
         ${CROWDB_KV_CLIENT_DIR}/include
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/crowdb-rpc-build/generated
)
find_package(Threads REQUIRED)
target_link_libraries(crowdb-diskio-lib PUBLIC Threads::Threads crowdbcommon crowdb-rpc)

# ── Binary ────────────────────────────────────────────────────────
add_executable(crowdb-diskio ${CMAKE_CURRENT_SOURCE_DIR}/src/dio_main.cpp)
target_include_directories(crowdb-diskio PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_BINARY_DIR}/crowdb-rpc-build/generated
)
target_link_libraries(crowdb-diskio PRIVATE crowdb-diskio-lib ${CROWDB_KV_CLIENT_FFI_LIB} ${CMAKE_DL_LIBS})
add_dependencies(crowdb-diskio crowdb-kv-client-ffi)
set_target_properties(crowdb-diskio PROPERTIES
  BUILD_RPATH ${CROWDB_ROOT_DIR}/target/${CARGO_PROFILE}
)

# ── Tests ─────────────────────────────────────────────────────────
enable_testing()
include(GoogleTest)
find_package(GTest REQUIRED)
file(GLOB_RECURSE CROWDB_DISKIO_TEST_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)
if(CROWDB_DISKIO_TEST_SRC)
  add_executable(crowdb_diskio_tests ${CROWDB_DISKIO_TEST_SRC})
  target_include_directories(crowdb_diskio_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/tests
    ${CMAKE_CURRENT_BINARY_DIR}/crowdb-rpc-build/generated
  )
  target_link_libraries(crowdb_diskio_tests PRIVATE crowdb-diskio-lib GTest::gtest GTest::gtest_main)
  gtest_discover_tests(crowdb_diskio_tests DISCOVERY_MODE PRE_TEST)
endif()

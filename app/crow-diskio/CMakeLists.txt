cmake_minimum_required(VERSION 3.20)
project(crow-diskio LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

add_compile_options(-Wall -Wextra -Werror)

# ── crow-rpc (RPC server, framing, flatbuffer C++ codegen) ────────
# Built in-tree so crow-diskio can link RpcServer and use the generated
# flatbuffer headers (diskio_generated.h etc.) from crow-rpc's build dir.
# crow-rpc itself pulls in crow-common via add_subdirectory, so we don't
# add crow-common separately here (avoids duplicate target collision).
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../../lib/crow-rpc crow-rpc-build)

# ── crow-kv-client FFI (Rust cdylib with --features ffi) ──────────
# crow-kv-client produces both an rlib (for Rust dependents) and a
# cdylib (for C++ consumers). The cdylib exports C ABI symbols only
# when built with the `ffi` feature.
set(CROW_KV_CLIENT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../lib/crow-kv-client)
set(CROW_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../..)
if(CMAKE_BUILD_TYPE STREQUAL "Release")
  set(CARGO_PROFILE release)
else()
  set(CARGO_PROFILE debug)
endif()
set(CROW_KV_CLIENT_FFI_LIB ${CROW_ROOT_DIR}/target/${CARGO_PROFILE}/libcrow_kv_client.so)
add_custom_command(
  OUTPUT ${CROW_KV_CLIENT_FFI_LIB}
  COMMAND pixi run cargo build -p crow-kv-client --features ffi
          $<$<STREQUAL:${CARGO_PROFILE},release>:--release>
  WORKING_DIRECTORY ${CROW_ROOT_DIR}
  COMMENT "Building crow-kv-client (Rust cdylib with ffi feature)"
  VERBATIM
)
add_custom_target(crow-kv-client-ffi DEPENDS ${CROW_KV_CLIENT_FFI_LIB})

# ── Source files (exclude dio_main.cpp from the library) ──────────
file(GLOB_RECURSE CROW_DISKIO_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
list(REMOVE_ITEM CROW_DISKIO_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/dio_main.cpp)

add_library(crow-diskio-lib STATIC ${CROW_DISKIO_SRC})
target_include_directories(crow-diskio-lib
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src
         ${CMAKE_CURRENT_SOURCE_DIR}/../../lib/crow-common/cpp/include
         ${CMAKE_CURRENT_SOURCE_DIR}/../../lib/crow-rpc/include
         ${CROW_KV_CLIENT_DIR}/include
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/crow-rpc-build/generated
)
find_package(Threads REQUIRED)
target_link_libraries(crow-diskio-lib PUBLIC Threads::Threads crowcommon crow-rpc)

# ── Binary ────────────────────────────────────────────────────────
add_executable(crow-diskio ${CMAKE_CURRENT_SOURCE_DIR}/src/dio_main.cpp)
target_include_directories(crow-diskio PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_BINARY_DIR}/crow-rpc-build/generated
)
target_link_libraries(crow-diskio PRIVATE crow-diskio-lib ${CROW_KV_CLIENT_FFI_LIB} ${CMAKE_DL_LIBS})
add_dependencies(crow-diskio crow-kv-client-ffi)
set_target_properties(crow-diskio PROPERTIES
  BUILD_RPATH ${CROW_ROOT_DIR}/target/${CARGO_PROFILE}
)

# ── Tests ─────────────────────────────────────────────────────────
enable_testing()
include(GoogleTest)
find_package(GTest REQUIRED)
file(GLOB_RECURSE CROW_DISKIO_TEST_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)
if(CROW_DISKIO_TEST_SRC)
  add_executable(crow_diskio_tests ${CROW_DISKIO_TEST_SRC})
  target_include_directories(crow_diskio_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/tests
    ${CMAKE_CURRENT_BINARY_DIR}/crow-rpc-build/generated
  )
  target_link_libraries(crow_diskio_tests PRIVATE crow-diskio-lib GTest::gtest GTest::gtest_main)
  gtest_discover_tests(crow_diskio_tests DISCOVERY_MODE PRE_TEST)
endif()

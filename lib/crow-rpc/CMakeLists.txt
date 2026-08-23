cmake_minimum_required(VERSION 3.20)
project(crow-rpc LANGUAGES CXX)

# ── Standard / warnings ───────────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

add_compile_options(-Wall -Wextra -Werror)

# ── Optional sanitizers: -DCROW_RPC_SANITIZER=address|thread|undefined ──
if(CROW_RPC_SANITIZER)
  message(STATUS "crow-rpc sanitizer: ${CROW_RPC_SANITIZER}")
  add_compile_options(-fsanitize=${CROW_RPC_SANITIZER} -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=${CROW_RPC_SANITIZER})
endif()

# ── Flatbuffer C++ codegen ────────────────────────────────────────
# Run flatc --cpp over crow-protocol's .fbs schemas into a build-time
# generated dir. The .fbs source files live in crow-protocol (single home
# for all proto types); the generated headers are build artifacts here.
# flatc is provided by pixi (conda-forge `flatbuffers` package).
find_program(FLATC_BIN flatc REQUIRED)
set(CROW_PROTOCOL_FBS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../crow-protocol/src/fbs)
set(CROW_RPC_PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${CROW_RPC_PROTO_GEN_DIR})
set(CROW_RPC_FBS_FILES
  ${CROW_PROTOCOL_FBS_DIR}/ret_code.fbs
  ${CROW_PROTOCOL_FBS_DIR}/msg_type.fbs
  ${CROW_PROTOCOL_FBS_DIR}/common_type.fbs
  ${CROW_PROTOCOL_FBS_DIR}/common_msg.fbs
  ${CROW_PROTOCOL_FBS_DIR}/diskio.fbs
)
execute_process(
  COMMAND ${FLATC_BIN} --cpp -o ${CROW_RPC_PROTO_GEN_DIR} ${CROW_RPC_FBS_FILES}
  RESULT_VARIABLE FLATC_RESULT
)
if(NOT FLATC_RESULT EQUAL 0)
  message(FATAL_ERROR "flatc --cpp failed for crow-rpc proto schemas (code ${FLATC_RESULT})")
endif()

# ── RDMA (libibverbs + librdmacm, Linux only) ─────────────────────
# Gated behind CROW_RPC_HAVE_RDMA. No macOS RNICs, so RDMA sources are
# excluded from the build entirely when not found (mirrors crow-tree's
# liburing gate). rdma-core (conda-forge) provides both libraries on Linux.
find_path(IBVERBS_INCLUDE_DIR infiniband/verbs.h)
find_library(IBVERBS_LIB NAMES ibverbs)
find_path(RDMACM_INCLUDE_DIR rdma/rdma_cma.h)
find_library(RDMACM_LIB NAMES rdmacm)
if(IBVERBS_INCLUDE_DIR AND IBVERBS_LIB AND RDMACM_INCLUDE_DIR AND RDMACM_LIB)
  set(CROW_RPC_HAVE_RDMA TRUE)
  message(STATUS "crow-rpc RDMA: enabled (${IBVERBS_LIB}, ${RDMACM_LIB})")
else()
  set(CROW_RPC_HAVE_RDMA FALSE)
  message(STATUS "crow-rpc RDMA: not found (TCP-only build)")
endif()

# ── Library ───────────────────────────────────────────────────────
file(GLOB_RECURSE CROW_RPC_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
if(NOT CROW_RPC_HAVE_RDMA)
  list(REMOVE_ITEM CROW_RPC_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/src/transport/rdma/rdma_buffer_pool.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/transport/rdma/rdma_transport.cpp
  )
endif()
# epoll_engine is Linux-only; kqueue_engine is macOS-only.
if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  list(REMOVE_ITEM CROW_RPC_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/transport/epoll/epoll_engine.cpp)
endif()
if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  list(REMOVE_ITEM CROW_RPC_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/transport/kqueue/kqueue_engine.cpp)
endif()
# ── crow-common (shared utils: crc32c, log, gzip, metrics) ────────
# Built in-tree via add_subdirectory so crow-rpc can link
# MetricsRegistry::global() and other crow-common symbols.
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../crow-common/cpp crow-common-build)

add_library(crow-rpc STATIC ${CROW_RPC_SRC})
target_include_directories(crow-rpc
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
          ${CMAKE_CURRENT_SOURCE_DIR}/../crow-common/cpp/include
  PRIVATE ${CROW_RPC_PROTO_GEN_DIR}
)
find_package(Threads REQUIRED)
target_link_libraries(crow-rpc PUBLIC Threads::Threads crowcommon)

# flatbuffers C++ runtime headers (header-only, no link needed).
# HINTS ENV CONDA_PREFIX: prefer the pixi/conda env's flatbuffers over any
# stale system copy (e.g. /usr/local) — generated schemas are emitted by the
# env's flatc and assert a matching FLATBUFFERS_VERSION_MAJOR.
find_path(FLATBUFFERS_INCLUDE_DIR flatbuffers/flatbuffers.h HINTS ENV CONDA_PREFIX PATH_SUFFIXES include)
if(FLATBUFFERS_INCLUDE_DIR)
  target_include_directories(crow-rpc SYSTEM PRIVATE ${FLATBUFFERS_INCLUDE_DIR})
endif()

# ── Boost (force the pixi/conda libboost, not a stale /opt/boost) ──
# folly-config.cmake calls find_package(Boost ... COMPONENTS ...). A broken
# /opt/boost (built against a newer glibc than the conda toolchain) ships a
# Boost CMake config that wins by default and causes undefined-reference link
# errors. Force CMake's FindBoost module (CMP0167=OLD) at the conda prefix and
# ignore /opt/boost so folly links the same libboost 1.90.0 it was built
# against. Set before find_package(folly) so folly's internal Boost lookup
# inherits these.
cmake_policy(SET CMP0167 OLD)
set(BOOST_ROOT $ENV{CONDA_PREFIX})
set(Boost_NO_BOOST_CMAKE ON)
list(APPEND CMAKE_IGNORE_PATH /opt/boost)

# ── folly (ConcurrentHashMap for pending map) ─────────────────────
# Required: the pending map uses folly::ConcurrentHashMap (striped locks)
# for the response hot path. No fallback to std::unordered_map.
find_package(folly CONFIG REQUIRED)
target_link_libraries(crow-rpc PUBLIC Folly::folly)
message(STATUS "crow-rpc folly: enabled (ConcurrentHashMap)")

if(CROW_RPC_HAVE_RDMA)
  target_include_directories(crow-rpc SYSTEM PRIVATE ${IBVERBS_INCLUDE_DIR} ${RDMACM_INCLUDE_DIR})
  target_compile_definitions(crow-rpc PUBLIC CROW_RPC_HAVE_RDMA=1)
  target_link_libraries(crow-rpc PUBLIC ${IBVERBS_LIB} ${RDMACM_LIB})
endif()

# ── Tests (GoogleTest) ────────────────────────────────────────────
enable_testing()
find_package(GTest REQUIRED)
include(GoogleTest)

file(GLOB_RECURSE CROW_RPC_TEST_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)
if(NOT CROW_RPC_HAVE_RDMA)
  list(REMOVE_ITEM CROW_RPC_TEST_SRC ${CMAKE_CURRENT_SOURCE_DIR}/tests/rdma_test.cpp)
endif()
add_executable(crow_rpc_tests ${CROW_RPC_TEST_SRC})
target_include_directories(crow_rpc_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/tests
    ${CROW_RPC_PROTO_GEN_DIR}
)
target_link_libraries(crow_rpc_tests PRIVATE crow-rpc GTest::gtest GTest::gtest_main)
gtest_discover_tests(crow_rpc_tests DISCOVERY_MODE PRE_TEST)

# ── Example: standalone echo server ────────────────────────────────
add_executable(crow-rpc-echo-server examples/echo_server.cpp)
target_link_libraries(crow-rpc-echo-server PRIVATE crow-rpc)

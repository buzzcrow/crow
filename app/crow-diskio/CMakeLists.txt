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

# ── crow-common (shared: reactor, log, metrics) ───────────────────
# Built in-tree via add_subdirectory so crow-diskio can link Reactor
# and other crow-common symbols (same pattern as crow-rpc).
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../../lib/crow-common/cpp crow-common-build)

# ── Source files ──────────────────────────────────────────────────
file(GLOB_RECURSE CROW_DISKIO_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

add_library(crow-diskio-lib STATIC ${CROW_DISKIO_SRC})
target_include_directories(crow-diskio-lib
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src
         ${CMAKE_CURRENT_SOURCE_DIR}/../../lib/crow-common/cpp/include
)
find_package(Threads REQUIRED)
target_link_libraries(crow-diskio-lib PUBLIC Threads::Threads crowcommon)

# ── Tests ─────────────────────────────────────────────────────────
enable_testing()
include(GoogleTest)
find_package(GTest REQUIRED)
file(GLOB_RECURSE CROW_DISKIO_TEST_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)
if(CROW_DISKIO_TEST_SRC)
  add_executable(crow_diskio_tests ${CROW_DISKIO_TEST_SRC})
  target_include_directories(crow_diskio_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
  target_link_libraries(crow_diskio_tests PRIVATE crow-diskio-lib GTest::gtest GTest::gtest_main)
  gtest_discover_tests(crow_diskio_tests DISCOVERY_MODE PRE_TEST)
endif()

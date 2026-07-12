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
file(GLOB_RECURSE CROWTREE_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc)
add_library(crowtree STATIC ${CROWTREE_SRC})
target_include_directories(crowtree PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
find_package(Threads REQUIRED)
target_link_libraries(crowtree PUBLIC Threads::Threads)

# ── Tests (GoogleTest) ────────────────────────────────────────────
enable_testing()
find_package(GTest REQUIRED)
include(GoogleTest)

file(GLOB_RECURSE CROWTREE_TEST_SRC CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cc)
add_executable(crowtree_tests ${CROWTREE_TEST_SRC})
target_link_libraries(crowtree_tests PRIVATE crowtree GTest::gtest GTest::gtest_main)
# PRE_TEST: enumerate tests at ctest time, not build time. Required so sanitizer
# builds (which can't run the binary at build time under restrictive ASLR) build
# cleanly; the suite is then run via `setarch -R` (see Makefile).
gtest_discover_tests(crowtree_tests DISCOVERY_MODE PRE_TEST)

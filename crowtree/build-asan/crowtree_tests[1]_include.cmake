if(EXISTS "/cjdata/cpp/crowkv/crowtree/build-asan/crowtree_tests")
  if(NOT EXISTS "/cjdata/cpp/crowkv/crowtree/build-asan/crowtree_tests[1]_tests.cmake" OR
     NOT "/cjdata/cpp/crowkv/crowtree/build-asan/crowtree_tests[1]_tests.cmake" IS_NEWER_THAN "/cjdata/cpp/crowkv/crowtree/build-asan/crowtree_tests" OR
     NOT "/cjdata/cpp/crowkv/crowtree/build-asan/crowtree_tests[1]_tests.cmake" IS_NEWER_THAN "${CMAKE_CURRENT_LIST_FILE}")
    include("/usr/share/cmake-3.28/Modules/GoogleTestAddTests.cmake")
    gtest_discover_tests_impl(
      TEST_EXECUTABLE [==[/cjdata/cpp/crowkv/crowtree/build-asan/crowtree_tests]==]
      TEST_EXECUTOR [==[]==]
      TEST_WORKING_DIR [==[/cjdata/cpp/crowkv/crowtree/build-asan]==]
      TEST_EXTRA_ARGS [==[]==]
      TEST_PROPERTIES [==[]==]
      TEST_PREFIX [==[]==]
      TEST_SUFFIX [==[]==]
      TEST_FILTER [==[]==]
      NO_PRETTY_TYPES [==[FALSE]==]
      NO_PRETTY_VALUES [==[FALSE]==]
      TEST_LIST [==[crowtree_tests_TESTS]==]
      CTEST_FILE [==[/cjdata/cpp/crowkv/crowtree/build-asan/crowtree_tests[1]_tests.cmake]==]
      TEST_DISCOVERY_TIMEOUT [==[5]==]
      TEST_XML_OUTPUT_DIR [==[]==]
    )
  endif()
  include("/cjdata/cpp/crowkv/crowtree/build-asan/crowtree_tests[1]_tests.cmake")
else()
  add_test(crowtree_tests_NOT_BUILT crowtree_tests_NOT_BUILT)
endif()

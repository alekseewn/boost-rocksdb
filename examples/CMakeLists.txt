function(add_example name source)
  add_executable(${name} ${source})
  target_include_directories(${name} PRIVATE ${PROJECT_SOURCE_DIR}/include)
  target_link_libraries(${name} ${ROCKSDB_STATIC_LIB})
endfunction()

set(BOOST_FIBER_SRC_DIR ${Boost_INCLUDE_DIR}/../libs/fiber/src)

if(NOT IS_DIRECTORY "${BOOST_FIBER_SRC_DIR}")
  set(BOOST_FIBER_SRC_DIR /home/alekseev/boost/libs/fiber/src)
endif()

set(BOOST_FIBER_SOURCES
  ${BOOST_FIBER_SRC_DIR}/algo/algorithm.cpp
  ${BOOST_FIBER_SRC_DIR}/algo/round_robin.cpp
  ${BOOST_FIBER_SRC_DIR}/algo/shared_work.cpp
  ${BOOST_FIBER_SRC_DIR}/algo/work_stealing.cpp
  ${BOOST_FIBER_SRC_DIR}/barrier.cpp
  ${BOOST_FIBER_SRC_DIR}/condition_variable.cpp
  ${BOOST_FIBER_SRC_DIR}/context.cpp
  ${BOOST_FIBER_SRC_DIR}/fiber.cpp
  ${BOOST_FIBER_SRC_DIR}/future.cpp
  ${BOOST_FIBER_SRC_DIR}/io_uring.cpp
  ${BOOST_FIBER_SRC_DIR}/mutex.cpp
  ${BOOST_FIBER_SRC_DIR}/properties.cpp
  ${BOOST_FIBER_SRC_DIR}/recursive_mutex.cpp
  ${BOOST_FIBER_SRC_DIR}/recursive_timed_mutex.cpp
  ${BOOST_FIBER_SRC_DIR}/scheduler.cpp
  ${BOOST_FIBER_SRC_DIR}/timed_mutex.cpp
  ${BOOST_FIBER_SRC_DIR}/waker.cpp
)

function(add_example_boost name source)
  add_executable(${name} ${source} ${BOOST_FIBER_SOURCES})
  target_include_directories(${name} PRIVATE ${PROJECT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/perf
    ${Boost_INCLUDE_DIR}
  )
  target_link_libraries(${name} ${ROCKSDB_STATIC_LIB}
    Boost::context ${CMAKE_THREAD_LIBS_INIT} uring gflags)
  target_compile_options(${name} PRIVATE -std=c++17 -Wno-unused-parameter -frtti)
endfunction()

add_example(simple_example simple_example.cc)
add_example(column_families_example column_families_example.cc)
add_example(compact_files_example compact_files_example.cc)
add_example(compaction_filter_example compaction_filter_example.cc)
add_example(optimistic_transaction_example optimistic_transaction_example.cc)
add_example(options_file_example options_file_example.cc)
add_example(transaction_example transaction_example.cc)
add_example(multi_processes_example multi_processes_example.cc)
add_example(c_simple_example c_simple_example.c)

add_example_boost(perf-client perf/perf-client.cpp)
add_example_boost(perf-server perf/perf-server.cpp)
add_example_boost(stress-put perf/stress-put.cpp)

function(add_example name source)
  add_executable(${name} ${source})
  target_include_directories(${name} PRIVATE ${PROJECT_SOURCE_DIR}/include)
  target_link_libraries(${name} ${ROCKSDB_STATIC_LIB})
endfunction()

function(add_example_boost name source)
  add_executable(${name} ${source})
  target_include_directories(${name} PRIVATE ${PROJECT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/perf)
  target_link_libraries(${name} ${ROCKSDB_STATIC_LIB}
    ${Boost_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} uring gflags)
  target_compile_options(${name} PRIVATE -std=c++17)
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

function(add_example_posix name source)
  add_executable(${name} ${source})
  target_include_directories(${name} PRIVATE ${PROJECT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/perf)
  target_link_libraries(${name} ${ROCKSDB_STATIC_LIB}
    ${CMAKE_THREAD_LIBS_INIT} gflags)
  target_compile_options(${name} PRIVATE -std=c++17)
endfunction()

add_example_posix(perf-client perf/perf-client.cpp)
add_example_boost(perf-server perf/perf-server.cpp)

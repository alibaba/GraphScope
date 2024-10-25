add_executable(wal_writer_test ${CMAKE_CURRENT_SOURCE_DIR}/wal_writer_test.cc)
target_link_libraries(wal_writer_test PUBLIC ${CppKafka_LIBRARIES} ${LIBGRAPELITE_LIBRARIES} flex_graph_db)
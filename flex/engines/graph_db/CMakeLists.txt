add_subdirectory(runtime)
file(GLOB_RECURSE GRAPH_DB_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/app/*.cc"
  	                             "${CMAKE_CURRENT_SOURCE_DIR}/database/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/database/wal/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/app/builtin/*.cc")

if (NOT BUILD_KAFKA_WAL_WRITER_PARSER)
        list(REMOVE_ITEM GRAPH_DB_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/database/wal/kafka_wal_writer.cc"
                                            "${CMAKE_CURRENT_SOURCE_DIR}/database/wal/kafka_wal_parser.cc")   
endif()

add_library(flex_graph_db SHARED ${GRAPH_DB_SRC_FILES})

target_include_directories(flex_graph_db PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
target_link_libraries(flex_graph_db flex_rt_mutable_graph ${LIBGRAPELITE_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(flex_graph_db runtime_execute)
if (BUILD_KAFKA_WAL_WRITER_PARSER)
        target_link_libraries(flex_graph_db ${CppKafka_LIBRARIES})
endif()
install_flex_target(flex_graph_db)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/database/graph_db.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/graph_db_session.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/insert_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/read_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/single_edge_insert_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/single_vertex_insert_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/update_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/compact_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/version_manager.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/transaction_utils.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/graph_db_operations.h
        DESTINATION include/flex/engines/graph_db/database)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/app/app_base.h
        DESTINATION include/flex/engines/graph_db/app)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/app/hqps_app.h
        DESTINATION include/flex/engines/graph_db/app)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/database/wal/wal.h
        ${CMAKE_CURRENT_SOURCE_DIR}/database/wal/local_wal_parser.h
        ${CMAKE_CURRENT_SOURCE_DIR}/database/wal/local_wal_writer.h
        DESTINATION include/flex/engines/graph_db/database/wal)




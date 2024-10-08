add_subdirectory(runtime)
file(GLOB_RECURSE GRAPH_DB_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/app/*.cc"
  	                             "${CMAKE_CURRENT_SOURCE_DIR}/database/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/app/builtin/*.cc")

add_library(flex_graph_db SHARED ${GRAPH_DB_SRC_FILES})

target_include_directories(flex_graph_db PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
target_link_libraries(flex_graph_db flex_rt_mutable_graph flex_utils ${LIBGRAPELITE_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(flex_graph_db flex_plan_proto runtime_adhoc)
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
              ${CMAKE_CURRENT_SOURCE_DIR}/database/wal.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/transaction_utils.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/graph_db_operations.h
        DESTINATION include/flex/engines/graph_db/database)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/app/app_base.h
        DESTINATION include/flex/engines/graph_db/app)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/app/hqps_app.h
        DESTINATION include/flex/engines/graph_db/app)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/app/adhoc_app.h
        DESTINATION include/flex/engines/graph_db/app)



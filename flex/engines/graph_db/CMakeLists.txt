file(GLOB_RECURSE GRAPH_DB_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/app/*.cc"
  	                             "${CMAKE_CURRENT_SOURCE_DIR}/database/*.cc")

add_library(flex_graph_db SHARED ${GRAPH_DB_SRC_FILES})

target_link_libraries(flex_graph_db flex_rt_mutable_graph flex_utils ${GLOG_LIBRARIES} ${LIBGRAPELITE_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

install(TARGETS flex_graph_db
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/database/graph_db.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/graph_db_session.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/insert_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/read_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/single_edge_insert_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/single_vertex_insert_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/update_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/version_manager.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/wal.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/transaction_utils.h
        DESTINATION include/flex/engines/graph_db/database)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/app/app_base.h
        DESTINATION include/flex/engines/graph_db/app)


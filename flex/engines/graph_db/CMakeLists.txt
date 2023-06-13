find_package (Hiactor)
if (Hiactor_FOUND)
  include (${Hiactor_CODEGEN_CMAKE_FILE})

  hiactor_codegen (actor_autogen actor_autogen_files
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/server
        INCLUDE_PATHS ${Hiactor_INCLUDE_DIR},${CMAKE_CURRENT_SOURCE_DIR}/../../../)

  file(GLOB_RECURSE GRAPH_DB_SERVER_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/server/*")

  add_library(flex_graph_db_server STATIC
        ${GRAPH_DB_SERVER_SRC_FILES}
        ${actor_autogen_files})
  add_dependencies(flex_graph_db_server actor_autogen)
  target_compile_options (flex_graph_db_server
        PUBLIC
        -Wno-attributes)
  # target_link_libraries(flex_graph_db_server Hiactor::hiactor ${LIBGRAPELITE_LIBRARIES} flex_utils flex_rt_mutable_graph ${GLOG_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
  target_link_libraries(flex_graph_db_server Hiactor::hiactor)
endif ()

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
        DESTINATION include/flex/engines/graph_db/database)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/app/app_base.h
              ${CMAKE_CURRENT_SOURCE_DIR}/app/app_utils.h
        DESTINATION include/flex/engines/graph_db/app)


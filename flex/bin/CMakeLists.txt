find_package (Hiactor)
if (Hiactor_FOUND)
  add_executable(rt_server rt_server.cc)
  target_link_libraries(rt_server flex_utils flex_rt_mutable_graph flex_graph_db flex_graph_db_server)
  
  install(TARGETS rt_server
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION lib
          LIBRARY DESTINATION lib)
endif ()

add_executable(rt_admin rt_admin.cc)
target_link_libraries(rt_admin flex_utils flex_rt_mutable_graph flex_graph_db)

install(TARGETS rt_admin
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

add_executable(flex_analytical_engine flex_analytical_engine.cc)
target_link_libraries(flex_analytical_engine flex_immutable_graph flex_bsp ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES})

install(TARGETS flex_analytical_engine
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

find_package(Hiactor)

if(Hiactor_FOUND)
        add_executable(rt_server rt_server.cc)
        target_link_libraries(rt_server flex_utils flex_rt_mutable_graph flex_graph_db flex_server)

        install(TARGETS rt_server
                RUNTIME DESTINATION bin
                ARCHIVE DESTINATION lib
                LIBRARY DESTINATION lib)
endif()

if(Hiactor_FOUND)
        include_directories(../engines/http_server)
        add_executable(rt_bench rt_bench.cc)
        target_link_libraries(rt_bench flex_utils flex_rt_mutable_graph flex_graph_db flex_server)

        install(TARGETS rt_bench
                RUNTIME DESTINATION bin
                ARCHIVE DESTINATION lib
                LIBRARY DESTINATION lib)
endif()

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

if(BUILD_HQPS)
        if(Hiactor_FOUND)
                add_executable(sync_server sync_server.cc)
                target_link_libraries(sync_server flex_utils flex_graph_db flex_server hqps_plan_proto ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES})

                install(TARGETS sync_server
                        RUNTIME DESTINATION bin
                        ARCHIVE DESTINATION lib
                        LIBRARY DESTINATION lib)
        endif()
        # install the script
        install(PROGRAMS load_plan_and_gen.sh DESTINATION bin)
endif()

find_package(Boost COMPONENTS program_options REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
add_executable(bulk_loader bulk_loader.cc)
target_link_libraries(bulk_loader flex_rt_mutable_graph flex_utils ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES} ${Boost_LIBRARIES})

install(TARGETS bulk_loader
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)
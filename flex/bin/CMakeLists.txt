if(Hiactor_FOUND)
        add_executable(rt_server rt_server.cc)
        target_link_libraries(rt_server flex_utils flex_rt_mutable_graph flex_graph_db flex_server)
        install_without_export_flex_target(rt_server)
endif()

if(Hiactor_FOUND)
        include_directories(../engines/http_server)
        add_executable(rt_bench rt_bench.cc)
        target_link_libraries(rt_bench flex_utils flex_rt_mutable_graph flex_graph_db flex_server)
        install_without_export_flex_target(rt_bench)
endif()

add_executable(rt_admin rt_admin.cc)
target_link_libraries(rt_admin flex_utils flex_rt_mutable_graph flex_graph_db)
install_without_export_flex_target(rt_admin)


add_executable(adhoc_runner adhoc_runner.cc)
target_link_libraries(adhoc_runner flex_utils flex_graph_db)
install_without_export_flex_target(adhoc_runner)

add_executable(flex_analytical_engine flex_analytical_engine.cc)
target_link_libraries(flex_analytical_engine flex_immutable_graph flex_bsp ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES})
install_without_export_flex_target(flex_analytical_engine)


if(Hiactor_FOUND)
        add_executable(interactive_server interactive_server.cc)
        target_link_libraries(interactive_server flex_utils flex_graph_db flex_server flex_plan_proto flex_utils ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES})
        if (OPENTELEMETRY_CPP_FOUND)
                target_link_libraries(interactive_server otel)
        endif()
        install_without_export_flex_target(interactive_server)
endif()
# install the script
install(PROGRAMS load_plan_and_gen.sh DESTINATION bin)


include_directories(${Boost_INCLUDE_DIRS})
add_executable(bulk_loader bulk_loader.cc)
target_link_libraries(bulk_loader flex_rt_mutable_graph flex_utils ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES} ${Boost_LIBRARIES})
install_without_export_flex_target(bulk_loader)

add_executable(stored_procedure_runner stored_procedure_runner.cc)
target_link_libraries(stored_procedure_runner flex_rt_mutable_graph flex_utils flex_graph_db ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES} ${Boost_LIBRARIES})
install_without_export_flex_target(stored_procedure_runner)
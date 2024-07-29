
find_package(Boost REQUIRED COMPONENTS system filesystem
    context program_options regex thread)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

add_executable(gen_code_from_plan gen_code_from_plan.cc)
target_link_libraries(gen_code_from_plan flex_rt_mutable_graph flex_utils hqps_plan_proto ${GLOG_LIBRARIES}  ${Boost_LIBRARIES})
install_flex_target(gen_code_from_plan)

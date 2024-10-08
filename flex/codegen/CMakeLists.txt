add_executable(gen_code_from_plan gen_code_from_plan.cc)
target_link_libraries(gen_code_from_plan flex_rt_mutable_graph flex_utils flex_plan_proto ${GLOG_LIBRARIES}  ${Boost_LIBRARIES})
install_flex_target(gen_code_from_plan)

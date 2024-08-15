file(GLOB_RECURSE COMMON_SOURCES "common/*.cc")
add_library(runtime_common SHARED ${COMMON_SOURCES})
target_link_libraries(runtime_common ${Boost_LIBRARIES} flex_rt_mutable_graph flex_utils hqps_plan_proto)
install_flex_target(runtime_common)

file(GLOB_RECURSE ADHOC_SOURCES "adhoc/*.cc")
add_library(runtime_adhoc SHARED ${ADHOC_SOURCES})
target_link_libraries(runtime_adhoc runtime_common)
install_flex_target(runtime_adhoc)


file(GLOB_RECURSE CODEGEN_SOURCES "codegen/*.cc" "common/types.cc" "common/utils.cc")
add_library(runtime_codegen SHARED ${CODEGEN_SOURCES})
target_link_libraries(runtime_codegen hqps_plan_proto flex_utils flex_rt_mutable_graph)

add_executable(codegen codegen/codegen.cc)
target_link_libraries(codegen runtime_codegen)

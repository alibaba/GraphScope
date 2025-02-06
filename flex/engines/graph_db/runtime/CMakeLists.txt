file(GLOB_RECURSE COMMON_SOURCES "common/*.cc")
add_library(runtime_common SHARED ${COMMON_SOURCES})
target_link_libraries(runtime_common flex_rt_mutable_graph flex_plan_proto)
install_flex_target(runtime_common)

file(GLOB_RECURSE EXECUTE_SOURCES "execute/*.cc" "utils/*.cc")
add_library(runtime_execute SHARED ${EXECUTE_SOURCES})
target_link_libraries(runtime_execute runtime_common)
install_flex_target(runtime_execute)

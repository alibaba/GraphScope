file(GLOB_RECURSE COMMON_SOURCES "common/*.cc")
add_library(runtime_common SHARED ${COMMON_SOURCES})
target_link_libraries(runtime_common ${Boost_LIBRARIES} flex_utils flex_plan_proto)
install_flex_target(runtime_common)

file(GLOB_RECURSE ADHOC_SOURCES "adhoc/*.cc")
add_library(runtime_adhoc SHARED ${ADHOC_SOURCES})
target_link_libraries(runtime_adhoc runtime_common)
install_flex_target(runtime_adhoc)


install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/flex/engines/graph_db
        FILES_MATCHING
        PATTERN "*.h"
)
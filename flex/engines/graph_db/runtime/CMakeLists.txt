file(GLOB_RECURSE COMMON_SOURCES "common/*.cc")
add_library(runtime_common SHARED ${COMMON_SOURCES})
target_link_libraries(runtime_common ${Boost_LIBRARIES} flex_rt_mutable_graph flex_utils hqps_plan_proto)


file(GLOB_RECURSE ADHOC_SOURCES "adhoc/*.cc")
add_library(runtime_adhoc SHARED ${ADHOC_SOURCES})
target_link_libraries(runtime_adhoc runtime_common)


add_library(adhoc_app STATIC adhoc_app.cc)
target_link_libraries(adhoc_app runtime_common runtime_adhoc)
install(TARGETS adhoc_app runtime_common runtime_adhoc
        EXPORT flex-targets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
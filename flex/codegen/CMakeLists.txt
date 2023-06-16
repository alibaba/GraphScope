#hqps related
add_executable(gen_query_from_plan gen_query_from_plan.cc)
target_link_libraries(gen_query_from_plan hqps_plan_proto ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES})
# to include generated proto files
target_include_directories(gen_query_from_plan PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../engines/hqps/)
install(TARGETS gen_query_from_plan
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/flex/
        FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.template"
)
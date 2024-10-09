
# file(GLOB_RECURSE GS_TEST_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
file(GLOB GS_TEST_FILES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

foreach(f ${GS_TEST_FILES})
        string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
        set(T_NAME ${CMAKE_MATCH_1})
        message(STATUS "Found graphscope test - " ${T_NAME})
        add_executable(${T_NAME} ${CMAKE_CURRENT_SOURCE_DIR}/${T_NAME}.cc)
        target_link_libraries(${T_NAME}  flex_plan_proto flex_rt_mutable_graph flex_graph_db  flex_utils ${GLOG_LIBRARIES} ${LIBGRAPELITE_LIBRARIES})
endforeach()

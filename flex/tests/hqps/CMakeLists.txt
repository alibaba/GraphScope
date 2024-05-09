
# file(GLOB_RECURSE GS_TEST_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
file(GLOB GS_TEST_FILES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
find_package (Hiactor)

if (BUILD_HQPS)
        foreach(f ${GS_TEST_FILES})
                string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
                set(T_NAME ${CMAKE_MATCH_1})
                message(STATUS "Found graphscope test - " ${T_NAME})
                add_executable(${T_NAME} ${CMAKE_CURRENT_SOURCE_DIR}/${T_NAME}.cc)
                target_link_libraries(${T_NAME}  hqps_plan_proto flex_rt_mutable_graph flex_graph_db  flex_utils ${GLOG_LIBRARIES} ${LIBGRAPELITE_LIBRARIES})
                # if f has signal in its name, then it is a signal test
                if(${T_NAME} MATCHES "actor_example")
                        target_link_libraries(${T_NAME} Hiactor::hiactor)
                endif()
        endforeach()
endif()
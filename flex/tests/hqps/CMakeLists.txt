
# file(GLOB_RECURSE GS_TEST_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
file(GLOB GS_TEST_FILES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
if (NOT BUILD_WITH_OSS)
        list(REMOVE_ITEM GS_TEST_FILES "oss_test.cc")
endif()

foreach(f ${GS_TEST_FILES})
        string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
        set(T_NAME ${CMAKE_MATCH_1})
        message(STATUS "Found graphscope test - " ${T_NAME})
        add_executable(${T_NAME} ${CMAKE_CURRENT_SOURCE_DIR}/${T_NAME}.cc)
        target_link_libraries(${T_NAME} flex_plan_proto  flex_graph_db flex_utils)
        if (BUILD_WITH_OSS)
                target_link_libraries(${T_NAME} cpp-sdk)
        endif()
endforeach()

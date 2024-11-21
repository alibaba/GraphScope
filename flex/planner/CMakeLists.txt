file(GLOB_RECURSE PLANNER_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(graph_planner SHARED ${PLANNER_SRC_FILES})
target_link_libraries(graph_planner PUBLIC flex_plan_proto ${CMAKE_JNI_LINKER_FLAGS} ${JAVA_JVM_LIBRARY} ${JNI_LIBRARIES})
target_include_directories(graph_planner PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
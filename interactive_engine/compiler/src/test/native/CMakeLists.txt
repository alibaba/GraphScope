cmake_minimum_required (VERSION 3.5)
project(graph-planner-test LANGUAGES CXX)

include(CheckLibraryExists)
include(GNUInstallDirs)

find_package(graph-planner REQUIRED)
include_directories(${GRAPH_PLANNER_INCLUDE_DIRS})



set(JAVA_AWT_INCLUDE_PATH NotNeeded)
find_package(JNI QUIET)
if (JNI_FOUND)
    include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
    include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})
else()
    message(FATAL_ERROR "JNI not found")
endif()

add_executable(test_graph_planner test_graph_planner.cc)
target_link_libraries(test_graph_planner PRIVATE ${GRAPH_PLANNER_LIBRARIES} ${JAVA_JVM_LIBRARY} ${JNI_LIBRARIES})
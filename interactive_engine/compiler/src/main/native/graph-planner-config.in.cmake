# - Config file for the GraphPlanner package
#
# It defines the following variables
#
#  GRAPH_PLANNER_INCLUDE_DIR         - include directory for graph planner
#  GRAPH_PLANNER_INCLUDE_DIRS        - include directories for graph planner
#  GRAPH_PLANNER_LIBRARIES           - libraries to link against

set(GRAPH_PLANNER_HOME "${CMAKE_CURRENT_LIST_DIR}/../../..")
include("${CMAKE_CURRENT_LIST_DIR}/graph-planner-targets.cmake")

set(GRAPH_PLANNER_LIBRARIES planner::graph_planner)
set(GRAPH_PLANNER_INCLUDE_DIR "${GRAPH_PLANNER_HOME}/include")
set(GRAPH_PLANNER_INCLUDE_DIRS "${GRAPH_PLANNER_INCLUDE_DIR}")

include(FindPackageMessage)
find_package_message(graph-planner
    "Found GraphPlanner: ${CMAKE_CURRENT_LIST_FILE} (found version \"@GRAPH_PLANNER_VERSION@\")"
    "GraphPlanner version: @GRAPH_PLANNER_VERSION@\nGRAPH_PLANNER_LIBRARIES: ${GRAPH_PLANNER_LIBRARIES}, include directories: ${GRAPH_PLANNER_INCLUDE_DIRS}"
)

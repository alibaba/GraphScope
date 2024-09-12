# - Config file for the Flex package
#
# It defines the following variables
#
#  FLEX_INCLUDE_DIR         - include directory for flex
#  FLEX_INCLUDE_DIRS        - include directories for flex
#  FLEX_LIBRARIES           - libraries to link against

set(FLEX_HOME "${CMAKE_CURRENT_LIST_DIR}/../../..")
include("${CMAKE_CURRENT_LIST_DIR}/flex-targets.cmake")

set(FLEX_LIBRARIES flex::flex_utils flex::flex_rt_mutable_graph flex::flex_graph_db flex::flex_bsp flex::flex_immutable_graph flex::flex_plan_proto)
set(FLEX_INCLUDE_DIR "${FLEX_HOME}/include")
set(FLEX_INCLUDE_DIRS "${FLEX_INCLUDE_DIR}")

include(FindPackageMessage)
find_package_message(flex
    "Found Flex: ${CMAKE_CURRENT_LIST_FILE} (found version \"@FLEX_VERSION@\")"
    "Flex version: @FLEX_VERSION@\nFlex libraries: ${FLEX_LIBRARIES}, include directories: ${FLEX_INCLUDE_DIRS}"
)

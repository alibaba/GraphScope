# - Config file for the graphscope-analytical package
#
# It defines the following variables
#
#  GRAPHSCOPE_ANALYTICAL_INCLUDE_DIR         - include directory for graphscope-analytical
#  GRAPHSCOPE_ANALYTICAL_INCLUDE_DIRS        - include directories for graphscope-analytical
#  GRAPHSCOPE_ANALYTICAL_LIBRARIES           - libraries to link against
#  GRAPHSCOPE_ANALYTICAL_ENGINE_EXECUTABLE         - the grape-engine executable

set(GRAPHSCOPE_ANALYTICAL_HOME "${CMAKE_CURRENT_LIST_DIR}/../../..")
include("${CMAKE_CURRENT_LIST_DIR}/graphscope-analytical-targets.cmake")

set(GRAPHSCOPE_ANALYTICAL_LIBRARIES gs_proto)
set(GRAPHSCOPE_ANALYTICAL_INCLUDE_DIR "${GRAPHSCOPE_ANALYTICAL_HOME}/include"
                                      "${GRAPHSCOPE_ANALYTICAL_HOME}/include/graphscope")
set(GRAPHSCOPE_ANALYTICAL_INCLUDE_DIRS "${GRAPHSCOPE_ANALYTICAL_INCLUDE_DIR}")
set(GRAPHSCOPE_ANALYTICAL_ENGINE_EXECUTABLE "${GRAPHSCOPE_ANALYTICAL_HOME}/bin/grape_engine")

set(GRAPHSCOPE_GCC_ABI_BACKWARDS_COMPATIBLE @GCC_ABI_BACKWARDS_COMPATIBLE@)

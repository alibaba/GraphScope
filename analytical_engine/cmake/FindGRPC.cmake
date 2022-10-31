# This file is taken from
#
#   https://github.com/IvanSafonov/grpc-cmake-example
#
# The original repository is open-sourced with the MIT license.
#
# Locate and configure the gRPC library
#
# Adds the following targets:
#
#  gRPC::gpr - GPR library
#  gRPC::grpc - gRPC library
#  gRPC::grpc++ - gRPC C++ library
#  gRPC::grpc++_reflection - gRPC C++ reflection library
#  gRPC::grpc_cpp_plugin - C++ generator plugin for Protocol Buffers
#

# Find gRPC include directory
find_path(GRPC_INCLUDE_DIR grpc/grpc.h)
mark_as_advanced(GRPC_INCLUDE_DIR)

# Find gGPR library
find_library(GPR_LIBRARY NAMES gpr)
mark_as_advanced(GRPC_GPR_LIBRARY)
add_library(gRPC::gpr UNKNOWN IMPORTED)
set_target_properties(gRPC::gpr PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${GRPC_INCLUDE_DIR}
    INTERFACE_LINK_LIBRARIES "-lpthread;-ldl"
    IMPORTED_LOCATION ${GPR_LIBRARY}
)

# Find gRPC library
find_library(GRPC_LIBRARY NAMES grpc)
mark_as_advanced(GRPC_LIBRARY)
add_library(gRPC::grpc UNKNOWN IMPORTED)
set_target_properties(gRPC::grpc PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${GRPC_INCLUDE_DIR}
    INTERFACE_LINK_LIBRARIES gRPC::gpr
    IMPORTED_LOCATION ${GRPC_LIBRARY}
)

# Find gRPC C++ library
find_library(GRPC_GRPC++_LIBRARY NAMES grpc++)
mark_as_advanced(GRPC_GRPC++_LIBRARY)
add_library(gRPC::grpc++ UNKNOWN IMPORTED)
set_target_properties(gRPC::grpc++ PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${GRPC_INCLUDE_DIR}
    INTERFACE_LINK_LIBRARIES gRPC::grpc
    IMPORTED_LOCATION ${GRPC_GRPC++_LIBRARY}
)

# Find gRPC C++ reflection library
find_library(GRPC_GRPC++_REFLECTION_LIBRARY NAMES grpc++_reflection)
mark_as_advanced(GRPC_GRPC++_REFLECTION_LIBRARY)
add_library(gRPC::grpc++_reflection UNKNOWN IMPORTED)
set_target_properties(gRPC::grpc++_reflection PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${GRPC_INCLUDE_DIR}
    INTERFACE_LINK_LIBRARIES gRPC::grpc++
    IMPORTED_LOCATION ${GRPC_GRPC++_REFLECTION_LIBRARY}
)

# Find gRPC CPP generator
find_program(GRPC_CPP_PLUGIN NAMES grpc_cpp_plugin)
mark_as_advanced(GRPC_CPP_PLUGIN)
add_executable(gRPC::grpc_cpp_plugin IMPORTED)
set_target_properties(gRPC::grpc_cpp_plugin PROPERTIES
    IMPORTED_LOCATION ${GRPC_CPP_PLUGIN}
)

file(
    WRITE "${CMAKE_BINARY_DIR}/get_gRPC_version.cc"
[====[
#include <grpc++/grpc++.h>
#include <iostream>
int main() {
  std::cout << grpc::Version(); // no newline to simplify CMake module
  return 0;
}
]====])

try_run(
    _gRPC_GET_VERSION_STATUS
    _gRPC_GET_VERSION_COMPILE_STATUS
    "${CMAKE_BINARY_DIR}"
    "${CMAKE_BINARY_DIR}/get_gRPC_version.cc"
    CMAKE_FLAGS
    -DCMAKE_CXX_STANDARD=11
    LINK_LIBRARIES
    gRPC::grpc++
    gRPC::grpc
    COMPILE_OUTPUT_VARIABLE _gRPC_GET_VERSION_COMPILE_OUTPUT
    RUN_OUTPUT_VARIABLE gRPC_VERSION)

file(REMOVE "${CMAKE_BINARY_DIR}/get_gRPC_version.cc")

include(${CMAKE_ROOT}/Modules/FindPackageHandleStandardArgs.cmake)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(gRPC
  FOUND_VAR gRPC_FOUND
  REQUIRED_VARS GRPC_LIBRARY GPR_LIBRARY GRPC_INCLUDE_DIR GRPC_GRPC++_REFLECTION_LIBRARY GRPC_CPP_PLUGIN
  VERSION_VAR gRPC_VERSION)

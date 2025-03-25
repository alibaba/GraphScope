# Copyright 2021 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cmake_minimum_required(VERSION 3.1)
project(COMPILER-JNI
        LANGUAGES CXX
        VERSION 0.0.1)

option(BUILD_TEST "Whether to build test" ON)

include(GNUInstallDirs)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,-rpath,$ORIGIN")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

set(JAVA_AWT_INCLUDE_PATH NotNeeded)
set(CMAKE_CXX_STANDARD 17)
find_package(JNI QUIET)
if (JNI_FOUND)
    include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
    include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})
else()
    message(FATAL_ERROR "JNI not found")
endif()

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

set(GIE_COMPILER_PROTO_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/../../../../executor/ir/proto/)
# proto files of gie compiler
set(CODE_GEN_PROTOBUF_FILES
       ${GIE_COMPILER_PROTO_DIR}/algebra.proto
        ${GIE_COMPILER_PROTO_DIR}/common.proto
        ${GIE_COMPILER_PROTO_DIR}/expr.proto
        ${GIE_COMPILER_PROTO_DIR}/physical.proto
        ${GIE_COMPILER_PROTO_DIR}/results.proto
        ${GIE_COMPILER_PROTO_DIR}/schema.proto
        ${GIE_COMPILER_PROTO_DIR}/type.proto
        ${GIE_COMPILER_PROTO_DIR}/stored_procedure.proto
        ${GIE_COMPILER_PROTO_DIR}/basic_type.proto
)


# proto gen for gie proto
protobuf_generate(APPEND_PATH
        TARGET ${LOCAL_EXE_NAME}
        LANGUAGE cpp
        OUT_VAR PROTO_SRCS_GIE
        PROTOS ${CODE_GEN_PROTOBUF_FILES}
        IMPORT_DIRS ${GIE_COMPILER_PROTO_DIR}
        PROTOC_OUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

file(GLOB_RECURSE PLANNER_SRC_FILES "graph_planner.cc")
add_library(graph_planner SHARED ${PLANNER_SRC_FILES} ${PROTO_SRCS_GIE})
target_link_libraries(graph_planner ${CMAKE_JNI_LINKER_FLAGS} ${JAVA_JVM_LIBRARY} ${JNI_LIBRARIES} ${Protobuf_LIBRARIES})
target_include_directories(graph_planner PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

if (BUILD_TEST)
    add_executable(test_graph_planner test/test.cc)
    target_link_libraries(test_graph_planner PRIVATE graph_planner)
endif()

# Define where the output library will be installed
install(TARGETS graph_planner
    EXPORT GraphPlannerTargets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

# Install header files
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/ DESTINATION include)

# Export CMake package configuration
install(EXPORT GraphPlannerTargets
    FILE GraphPlannerTargets.cmake
    NAMESPACE GraphPlanner::
    DESTINATION lib/cmake/GraphPlanner
)

# Create a GraphPlannerConfig.cmake file for find_package()
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/GraphPlannerConfigVersion.cmake"
    VERSION 1.0.0
    COMPATIBILITY AnyNewerVersion
)

configure_package_config_file(
    "${CMAKE_SOURCE_DIR}/cmake/GraphPlannerConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/GraphPlannerConfig.cmake"
    INSTALL_DESTINATION lib/cmake/GraphPlanner
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/GraphPlannerConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/GraphPlannerConfigVersion.cmake"
    DESTINATION lib/cmake/GraphPlanner
)

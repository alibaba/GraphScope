cmake_minimum_required(VERSION 3.10)
project(lgraph)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_BUILD_TYPE
    "${CMAKE_BUILD_TYPE}"
    CACHE
    STRING
    "Choose the type of build."
    FORCE)
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE "Release")
endif ()

set(ROOT ${PROJECT_SOURCE_DIR})
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(BUILD_DIR ${ROOT}/build/debug)
else ()
    set(BUILD_DIR ${ROOT}/build/release)
    set(CMAKE_CXX_FLAGS_RELEASE "-O2")
endif()
set(LIBRARY_OUTPUT_PATH ${BUILD_DIR})

include_directories(${ROOT})

# Cmake find modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
find_package(Protobuf REQUIRED)

# proto file generating
set(PROTO_SRC_DIR ${ROOT}/../proto)
set(PROTOS
    ${PROTO_SRC_DIR}/model.proto
    ${PROTO_SRC_DIR}/sdk/common.proto
)
set(PROTO_GEN_DIR ${CMAKE_CURRENT_SOURCE_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_SRC_DIR} ${PROTO_GEN_DIR} ${PROTOS})
include_directories(${PROTO_GEN_DIR})

# Maxgraph Ffi Dependency
if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(DYLIB_SUFFIX "dylib")
else ()
    set(DYLIB_SUFFIX "so")
endif()

set(EXECUTOR_DIR ${ROOT}/../executor)
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(MAXGRAPH_FFI_LIB_DIR ${EXECUTOR_DIR}/target/debug)
else ()
    set(MAXGRAPH_FFI_LIB_DIR ${EXECUTOR_DIR}/target/release)
endif()

# Add library
set(HEADER_FILES
    include/common/namespace.h
    include/common/types.h
    include/db/edge.h
    include/db/error.h
    include/db/property.h
    include/db/readonly_db.h
    include/db/snapshot.h
    include/db/vertex.h
    include/util/result.h
    jna/native.h
    jna/ffi_test.h
    store_ffi/store_ffi.h)
set(SOURCE_FILES
    src/jna/native.cc
    src/jna/ffi_test.cc
    src/edge.cc
    src/error.cc
    src/property.cc
    src/readonly_db.cc
    src/snapshot.cc
    src/vertex.cc)

add_library(lgraph SHARED
    ${HEADER_FILES}
    ${SOURCE_FILES}
    ${PROTO_SRCS}
    ${PROTO_HDRS}
    ${GRPC_SRCS}
    ${GRPC_HDRS})
set_target_properties(lgraph PROPERTIES CLEAN_DIRECT_OUTPUT 1)

target_include_directories(lgraph
    PUBLIC
        ${ROOT}/include
        ${ROOT}/jna
    PRIVATE
        ${ROOT}/src
        ${ROOT}/store_ffi
        ${PROTO_SRC_DIR})

target_link_libraries(lgraph
    PUBLIC
        ${MAXGRAPH_FFI_LIB_DIR}/libmaxgraph_ffi.${DYLIB_SUFFIX}
        protobuf::libprotobuf)

# Tests
add_executable(vertex_scan_test
        tests/test_scan_vertex.cc)
target_link_libraries(vertex_scan_test PUBLIC lgraph)

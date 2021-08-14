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

set(HEADER_FILES
    include/db/common/namespace.h
    include/db/common/types.h
    include/db/graph/edge.h
    include/db/graph/error.h
    include/db/graph/property.h
    include/db/graph/snapshot.h
    include/db/graph/vertex.h
    include/db/util/result.h
    jna/native.h
    jna/ffi_test.h
    store_ffi/store_ffi.h)
set(SOURCE_FILES
    src/edge.cc
    src/error.cc
    src/property.cc
    src/snapshot.cc
    src/vertex.cc
    src/jna/native.cc
    src/jna/ffi_test.cc)

add_library(lgraph SHARED
    ${HEADER_FILES}
    ${SOURCE_FILES})
set_target_properties(lgraph PROPERTIES CLEAN_DIRECT_OUTPUT 1)

target_include_directories(lgraph
    PUBLIC
        ${ROOT}/include
        ${ROOT}/jna
    PRIVATE
        ${ROOT}/src
        ${ROOT}/store_ffi)

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

target_link_libraries(lgraph PUBLIC ${MAXGRAPH_FFI_LIB_DIR}/libmaxgraph_ffi.${DYLIB_SUFFIX})

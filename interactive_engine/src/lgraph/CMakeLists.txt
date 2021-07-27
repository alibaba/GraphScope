cmake_minimum_required(VERSION 3.10)
project(lgraph)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS_RELEASE "-O2")

set(ROOT ${PROJECT_SOURCE_DIR})
set(LIBRARY_OUTPUT_PATH ${ROOT}/lib)

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
    jna/test.h)
set(SOURCE_FILES
    src/edge.cc
    src/error.cc
    src/property.cc
    src/rust_store_ffi.h
    src/snapshot.cc
    src/vertex.cc
    src/jna/native.cc
    src/jna/test.cc)

add_library(lgraph SHARED
    ${HEADER_FILES}
    ${SOURCE_FILES})
set_target_properties(lgraph PROPERTIES CLEAN_DIRECT_OUTPUT 1)

target_link_libraries(lgraph
    PUBLIC ${ROOT}/../executor/target/release/libmaxgraph_store.so)

target_include_directories(lgraph
    PUBLIC
        ${ROOT}/include
        ${ROOT}/jna
    PRIVATE
        ${ROOT}/src)

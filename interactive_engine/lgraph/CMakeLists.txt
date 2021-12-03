cmake_minimum_required(VERSION 3.10)
project(Lgraph)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS_RELEASE "-O2")

set(Lgraph_VERSION_MAJOR 0)
set(Lgraph_VERSION_MINOR 1)
set(Lgraph_VERSION_REVISION 0)
set(Lgraph_VERSION "${Lgraph_VERSION_MAJOR}.${Lgraph_VERSION_MINOR}.${Lgraph_VERSION_REVISION}")

set(Lgraph_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(Lgraph_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR})

option(Lgraph_BUILD_SHARED "Build lgraph as a shared library." ON)
option(Lgraph_INSTALL "Install targets." ON)

if (Lgraph_BUILD_SHARED)
  message(STATUS "Build will generate a shared library. "
    "Use Lgraph_BUILD_SHARED=0 to perform a static build")
  set(Lgraph_LIBRARY_TYPE SHARED)
else ()
  message(STATUS "Build will generate a static library.")
  set(Lgraph_LIBRARY_TYPE STATIC)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif ()

# Cmake find modules
list(APPEND CMAKE_MODULE_PATH "${Lgraph_SOURCE_DIR}/cmake")
find_package(Protobuf 3.10.0 REQUIRED)
find_package(gRPC 1.26.0 REQUIRED)
find_package(CppKafka 0.3.1 REQUIRED)

# Proto file generating
set(PROTO_FILE_DIR ${Lgraph_SOURCE_DIR}/../proto)
set(GRPC_FILES
  ${PROTO_FILE_DIR}/client.proto
  ${PROTO_FILE_DIR}/client_backup_service.proto)
set(PROTO_FILES
  ${GRPC_FILES}
  ${PROTO_FILE_DIR}/model.proto
  ${PROTO_FILE_DIR}/sdk/common.proto)
set(PROTO_BUILD_DIR ${Lgraph_BINARY_DIR}/generated)
set(PROTO_SRCS_DEST_DIR ${Lgraph_SOURCE_DIR}/src/proto)
set(PROTO_HDRS_DEST_DIR ${Lgraph_SOURCE_DIR}/include/lgraph/proto)
file(MAKE_DIRECTORY ${PROTO_BUILD_DIR})
file(MAKE_DIRECTORY ${PROTO_SRCS_DEST_DIR}/sdk)
file(MAKE_DIRECTORY ${PROTO_HDRS_DEST_DIR}/sdk)
protobuf_generate_cpp(
  PROTO_SRCS
  PROTO_HDRS
  ${PROTO_FILE_DIR}
  ${PROTO_BUILD_DIR}
  ${PROTO_GEN_DIR}
  ${PROTO_SRCS_DEST_DIR}
  ${PROTO_HDRS_DEST_DIR}
  ${PROTO_FILES})
grpc_generate_cpp(
  GRPC_SRCS
  GRPC_HDRS
  ${PROTO_FILE_DIR}
  ${PROTO_BUILD_DIR}
  ${PROTO_GEN_DIR}
  ${PROTO_SRCS_DEST_DIR}
  ${PROTO_HDRS_DEST_DIR}
  ${GRPC_FILES})
include_directories(${PROTO_HDRS_DEST_DIR})

# Maxgraph Ffi Dependency
if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  set(DYLIB_SUFFIX "dylib")
else ()
  set(DYLIB_SUFFIX "so")
endif ()

set(Maxgraph_FFI_LIB_DIR ${Lgraph_SOURCE_DIR}/../executor/target)
set(Maxgraph_FFI_LIB ${Maxgraph_FFI_LIB_DIR}/libmaxgraph_ffi.${DYLIB_SUFFIX})

# Add library
set(Lgraph_PROJECT_FILES
  include/lgraph/client/graph_client.h
  include/lgraph/common/backup_info.h
  include/lgraph/common/check.h
  include/lgraph/common/logger_info.h
  include/lgraph/common/namespace.h
  include/lgraph/common/schema.h
  include/lgraph/common/types.h
  include/lgraph/db/store_ffi/store_ffi.h
  include/lgraph/db/edge.h
  include/lgraph/db/error.h
  include/lgraph/db/property.h
  include/lgraph/db/readonly_db.h
  include/lgraph/db/snapshot.h
  include/lgraph/db/vertex.h
  include/lgraph/log_subscription/message.h
  include/lgraph/log_subscription/operation.h
  include/lgraph/log_subscription/subscriber.h
  include/lgraph/util/endian.h
  include/lgraph/util/result.h
  include/lgraph/jna/native.h
  include/lgraph/jna/ffi_test.h
  src/client/graph_client.cc
  src/db/edge.cc
  src/db/error.cc
  src/db/property.cc
  src/db/readonly_db.cc
  src/db/snapshot.cc
  src/db/vertex.cc
  src/jna/native.cc
  src/jna/ffi_test.cc
  src/log_subscription/operation.cc
  src/log_subscription/subscriber.cc
  src/check.cc)
add_library(lgraph ${Lgraph_LIBRARY_TYPE}
  ${Lgraph_PROJECT_FILES}
  ${PROTO_SRCS}
  ${PROTO_HDRS}
  ${GRPC_SRCS}
  ${GRPC_HDRS})
add_library(Lgraph::lgraph ALIAS lgraph)
set_target_properties(lgraph PROPERTIES CLEAN_DIRECT_OUTPUT 1)
target_include_directories(lgraph
  PUBLIC
    $<INSTALL_INTERFACE:include>
    $<BUILD_INTERFACE:${Lgraph_SOURCE_DIR}/include>
  PRIVATE
    ${Lgraph_SOURCE_DIR}/src)
target_link_libraries(lgraph
  PUBLIC
    ${Maxgraph_FFI_LIB}
    gRPC::grpc++_reflection
    protobuf::libprotobuf
    CppKafka::cppkafka)

# Tests
add_subdirectory(examples)

# Installation and export
if (Lgraph_INSTALL)
    include(GNUInstallDirs)
    include(CMakePackageConfigHelpers)

    set(Lgraph_CONFIG_INSTALL_DIR ${CMAKE_INSTALL_LIBDIR}/cmake/Lgraph)

    install(
      DIRECTORY ${Lgraph_SOURCE_DIR}/include/
      DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

    install(
      TARGETS lgraph
      EXPORT lgraph-export
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})

    install(
      EXPORT lgraph-export
      FILE LgraphTargets.cmake
      NAMESPACE Lgraph::
      DESTINATION ${Lgraph_CONFIG_INSTALL_DIR})

    install(
      FILES ${Maxgraph_FFI_LIB}
      DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )

    configure_package_config_file(
      ${Lgraph_SOURCE_DIR}/cmake/LgraphConfig.cmake.in
      ${Lgraph_BINARY_DIR}/LgraphConfig.cmake
      INSTALL_DESTINATION ${Lgraph_CONFIG_INSTALL_DIR})

    write_basic_package_version_file(
      ${Lgraph_BINARY_DIR}/LgraphConfigVersion.cmake
      VERSION ${Lgraph_VERSION}
      COMPATIBILITY ExactVersion)

    install(
      FILES
        ${Lgraph_BINARY_DIR}/LgraphConfig.cmake
        ${Lgraph_BINARY_DIR}/LgraphConfigVersion.cmake
        ${Lgraph_SOURCE_DIR}/cmake/FindgRPC.cmake
        ${Lgraph_SOURCE_DIR}/cmake/FindProtobuf.cmake
      DESTINATION ${Lgraph_CONFIG_INSTALL_DIR}
      COMPONENT config)
endif ()

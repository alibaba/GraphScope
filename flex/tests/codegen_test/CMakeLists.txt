cmake_minimum_required(VERSION 3.5)

include(FindPackageHandleStandardArgs)

project(HighQpsCodeGen
        VERSION 1.0
        LANGUAGES CXX)

# the query name, 
if (QUERY_NAME)
        message("Query name ${QUERY_NAME}")
else()
        message(FATAL_ERROR "QueryName not set")
endif()

# the path where we can find codegen headers.
if (CODEGEN_INSTALL_DIR)
        message("Codegen install dir ${CODEGEN_INSTALL_DIR}")
else()
        message(FATAL_ERROR "Codegen install dir not set")
endif()

# the path where we can find graphscope headers and graphscope libs.
if (GRAPHSCOPE_INSTALL_DIR)
        message("graphscope install dir ${GRAPHSCOPE_INSTALL_DIR}")
else()
        message(FATAL_ERROR "graphscope install dir not set")
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp -Wl,-rpath,$ORIGIN -O3 -flto -Werror=unused-result")

# find protobuf
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# needed in as we include grape/grape.h
find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_CXX_INCLUDE_PATH})

# needed in as we include grape/grape.h
find_package(libgrapelite REQUIRED)
include_directories(SYSTEM ${LIBGRAPELITE_INCLUDE_DIRS})

# copied from findGLOG.cmake
set(GLOG_ROOT_DIR "" CACHE PATH "Folder contains Google glog")

# rocksdb needed by grock
find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h PATHS "")
message(STATUS ${ROCKSDB_INCLUDE_DIR})
include_directories(${ROCKSDB_INCLUDE_DIR})
find_library(ROCKSDB_LIB rocksdb)

if(WIN32)
    find_path(GLOG_INCLUDE_DIR glog/logging.h
        PATHS ${GLOG_ROOT_DIR}/src/windows)
else()
    find_path(GLOG_INCLUDE_DIR glog/logging.h
        PATHS ${GLOG_ROOT_DIR})
endif()

if(MSVC)
    find_library(GLOG_LIBRARY_RELEASE libglog_static
        PATHS ${GLOG_ROOT_DIR}
        PATH_SUFFIXES Release)

    find_library(GLOG_LIBRARY_DEBUG libglog_static
        PATHS ${GLOG_ROOT_DIR}
        PATH_SUFFIXES Debug)

    set(GLOG_LIBRARY optimized ${GLOG_LIBRARY_RELEASE} debug ${GLOG_LIBRARY_DEBUG})
else()
    find_library(GLOG_LIBRARY glog
        PATHS ${GLOG_ROOT_DIR}
        PATH_SUFFIXES lib lib64)
endif()

find_package_handle_standard_args(Glog DEFAULT_MSG GLOG_INCLUDE_DIR GLOG_LIBRARY)

if(GLOG_FOUND)
  set(GLOG_INCLUDE_DIRS ${GLOG_INCLUDE_DIR})
  set(GLOG_LIBRARIES ${GLOG_LIBRARY})
  message(STATUS "Found glog    (include: ${GLOG_INCLUDE_DIR}, library: ${GLOG_LIBRARY})")
  mark_as_advanced(GLOG_ROOT_DIR GLOG_LIBRARY_RELEASE GLOG_LIBRARY_DEBUG
                                 GLOG_LIBRARY GLOG_INCLUDE_DIR)
endif()

if(NOT GLOG_FOUND)
        message(FATAL_ERROR "glog not found, please install the glog library")
else()
        include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
endif()

add_library(${QUERY_NAME} SHARED ${PROJECT_SOURCE_DIR}/${QUERY_NAME}.cc)
target_include_directories(${QUERY_NAME} PUBLIC ${CODEGEN_INSTALL_DIR} ${GRAPHSCOPE_INSTALL_DIR} ${GRAPHSCOPE_INSTALL_DIR}/build/)
# link_directories(${GRAPHSCOPE_INSTALL_DIR}/build/)
target_link_directories(${QUERY_NAME} PUBLIC ${GRAPHSCOPE_INSTALL_DIR}/build/)
target_link_libraries(${QUERY_NAME} PUBLIC physical_plan_proto graphscope ${GLOG_LIBRARIES} ${Protobuf_LIBRARIES} ${ROCKSDB_LIB})
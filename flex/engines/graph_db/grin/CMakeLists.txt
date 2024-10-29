cmake_minimum_required(VERSION 3.1)

set(GRIN_READER_MAJOR_VERSION 0)
set(GRIN_READER_MINOR_VERSION 1)
set(GRIN_READER_VERSION ${GRIN_READER_MAJOR_VERSION}.${GRIN_READER_MINOR_VERSION})

project(grin_reader LANGUAGES C CXX VERSION ${GRIN_READER_VERSION})

# Set flags
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99")

# ------------------------------------------------------------------------------
# find_libraries
# ------------------------------------------------------------------------------

find_package(libgrapelite REQUIRED)
include_directories(${LIBGRAPELITE_INCLUDE_DIRS})

include("../../../../flex/cmake/FindGFlags.cmake")
if (GFLAGS_FOUND)
    include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
else ()
    message(FATAL_ERROR "gflags not found")
endif ()

include("../../../../flex/cmake/FindGlog.cmake")
include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
if (GLOG_FOUND)
    set(CMAKE_REQUIRED_INCLUDES "${GLOG_INCLUDE_DIRS}")
    set(CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES} ${GLOG_LIBRARIES}")
endif ()

#find arrow----------------------------------------------------------------------
find_package(Arrow REQUIRED)
if (NOT Arrow_FOUND)
    message(FATAL_ERROR "arrow not found, please install the arrow library")
else ()
    include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
    if (TARGET arrow_shared)
        set(ARROW_SHARED_LIB arrow_shared)
    endif()
    if (TARGET arrow_static)
        set(ARROW_STATIC_LIB arrow_static)
    endif()
endif ()
# include odps cpp sdk
include_directories(SYSTEM ${CMAKE_CURRENT_SOURCE_DIR}/../../../third_party/odps/include/)
add_definitions(-DRAPIDJSON_HAS_CXX11=1)
add_definitions(-DRAPIDJSON_HAS_STDSTRING=1)
add_definitions(-DRAPIDJSON_HAS_CXX11_RVALUE_REFS=1)
add_definitions(-DRAPIDJSON_HAS_CXX11_RANGE_FOR=1)

find_package(yaml-cpp REQUIRED)
include_directories(SYSTEM ${yaml-cpp_INCLUDE_DIRS})

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../storages/rt_mutable_graph)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/property)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/../../../build/utils)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../build/utils)
else ()
    message(FATAL_ERROR "Please build flex first")
endif ()

file(GLOB_RECURSE FILES_NEED_FORMAT "src/*.cc")

add_custom_target(grin_clformat
        COMMAND clang-format --style=file -i ${FILES_NEED_FORMAT}
        COMMENT "Running clang-format."
        VERBATIM)
file(GLOB_RECURSE SRC_SOURCES "src/*.cc")
file(GLOB_RECURSE UTILS_SOURCES "../../../utils/*.cc")
file(GLOB_RECURSE STORAGE_SOURCES "../../../storages/rt_mutable_graph/*.cc")
set(SOURCES ${SRC_SOURCES} ${UTILS_SOURCES} ${STORAGE_SOURCES})

add_library(flex_grin SHARED ${SOURCES})
target_link_libraries(flex_grin ${LIBGRAPELITE_LIBRARIES} ${GFLAGS_LIBRARIES} ${CMAKE_DL_LIBS}  ${YAML_CPP_LIBRARIES})
if (ARROW_SHARED_LIB)
        target_link_libraries(flex_grin ${ARROW_SHARED_LIB})
else()
        target_link_libraries(flex_grin ${ARROW_STATIC_LIB})
endif()

add_executable(run_grin_test test/test.c)

target_include_directories(run_grin_test PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps fragment)
target_link_libraries(run_grin_test flex_grin ${LIBGRAPELITE_LIBRARIES} ${GFLAGS_LIBRARIES} ${CMAKE_DL_LIBS})


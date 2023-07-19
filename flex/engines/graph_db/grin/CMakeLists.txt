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

find_package(yaml-cpp REQUIRED)
include_directories(SYSTEM ${yaml-cpp_INCLUDE_DIRS})

#set(yaml-cpp_INCLUDE_DIRS "/usr/local/Cellar/yaml-cpp/0.7.0/include")
#include_directories(SYSTEM ${yaml-cpp_INCLUDE_DIRS})
#set(YAML_CPP_LIBRARIES "/usr/local/Cellar/yaml-cpp/0.7.0/lib/libyaml-cpp.0.7.0.dylib")


include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../..)
message(STATUS "${CMAKE_CURRENT_SOURCE_DIR}")
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../storages/rt_mutable_graph)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/property)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

file(GLOB_RECURSE FILES_NEED_FORMAT "src/*.cc")

add_custom_target(grin_clformat
        COMMAND clang-format --style=file -i ${FILES_NEED_FORMAT}
        COMMENT "Running clang-format."
        VERBATIM)

file(GLOB SOURCES "src/*.cc" "src/topology/*.cc" "src/property/*.cc" "src/index/*.cc" "src/common/*.cc" "../../../utils/property/*.cc" "../../../storages/rt_mutable_graph/*.cc")
add_library(flex_grin SHARED ${SOURCES})
target_link_libraries(flex_grin ${LIBGRAPELITE_LIBRARIES} ${GFLAGS_LIBRARIES} ${CMAKE_DL_LIBS}  ${YAML_CPP_LIBRARIES})


add_executable(run_grin_test test/test.c)

target_include_directories(run_grin_test PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps fragment)
target_link_libraries(run_grin_test flex_grin ${LIBGRAPELITE_LIBRARIES} ${GFLAGS_LIBRARIES} ${CMAKE_DL_LIBS})
target_link_libraries(run_grin_test)

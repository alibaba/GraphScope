cmake_minimum_required (VERSION 3.5)

file(READ ${CMAKE_CURRENT_SOURCE_DIR}/../VERSION FLEX_VERSION)
# Strip trailing newline
string(REGEX REPLACE "\n$" "" FLEX_VERSION "${FLEX_VERSION}")

project (
  Flex
  VERSION ${FLEX_VERSION}
  LANGUAGES CXX)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../)

set(DEFAULT_BUILD_TYPE "Release")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -mno-avx512f")

add_compile_definitions(FLEX_VERSION="${FLEX_VERSION}")

if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
else ()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp -Werror -Wl,-rpath,$ORIGIN")
endif ()

find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_CXX_INCLUDE_PATH})

find_package(libgrapelite REQUIRED)
include_directories(SYSTEM ${LIBGRAPELITE_INCLUDE_DIRS})
include_directories(SYSTEM ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps)

# find Threads------------------------------------------------------------------
set(CMAKE_THREAD_PREFER_PTHREAD ON)
find_package(Threads REQUIRED)

# find glog---------------------------------------------------------------------
include("cmake/FindGlog.cmake")
if (NOT GLOG_FOUND)
    message(FATAL_ERROR "glog not found, please install the glog library")
else ()
    include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
endif ()

# find gflags-------------------------------------------------------------------
include("cmake/FindGFlags.cmake")
if (NOT GFLAGS_FOUND)
    message(STATUS "gflags not found, build without gflags")
else ()
    include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
endif ()

# Find Doxygen
find_package(Doxygen)
# Add a target to generate the documentation
if(DOXYGEN_FOUND)
    set(DOXYGEN_IN ${CMAKE_CURRENT_SOURCE_DIR}/docs/Doxyfile.in)
    set(DOXYGEN_OUT ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
    configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
    add_custom_target(doc ALL
            COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
            WORKING_DIRECTORY ..
            COMMENT "Generating API documentation with Doxygen"
            VERBATIM)
endif(DOXYGEN_FOUND)

add_subdirectory(utils)
add_subdirectory(storages)
add_subdirectory(engines)
add_subdirectory(bin)


set(CPACK_PACKAGE_NAME "graphscope_flex")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Flex module of GraphScope")
set(CPACK_PACKAGE_VENDOR "GraphScope")
set(CPACK_PACKAGE_VERSION ${FLEX_VERSION})
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_CONTACT "graphscope@alibaba-inc.com")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "graphscope")
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)
set(CPACK_DEB_COMPONENT_INSTALL YES)

include(CPack)

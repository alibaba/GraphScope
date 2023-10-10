cmake_minimum_required (VERSION 3.5)

file(READ ${CMAKE_CURRENT_SOURCE_DIR}/../VERSION FLEX_VERSION)
# Strip trailing newline
string(REGEX REPLACE "\n$" "" FLEX_VERSION "${FLEX_VERSION}")

project (
  Flex
  VERSION ${FLEX_VERSION}
  LANGUAGES CXX)


option(BUILD_HQPS "Whether to build HighQPS Engine" ON)
option(BUILD_TEST "Whether to build test" ON)
option(BUILD_DOC "Whether to build doc" ON)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../)

set(DEFAULT_BUILD_TYPE "Release")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -mno-avx512f -fPIC")
set(CMAKE_CXX_FLAGS_DEBUG "-g3 -O0")


add_compile_definitions(FLEX_VERSION="${FLEX_VERSION}")

if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
else ()
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp -Wl,-rpath,$ORIGIN -lopen-pal")
    else ()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp -Werror -Wl,-rpath,$ORIGIN")
    endif ()
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

#find boost----------------------------------------------------------------------
find_package(Boost REQUIRED COMPONENTS system filesystem
             # required by folly
             context program_options regex thread)

#find arrow----------------------------------------------------------------------
include("cmake/FindArrow.cmake")
if (NOT ARROW_FOUND)
    message(FATAL_ERROR "arrow not found, please install the arrow library")
else ()
    include_directories(SYSTEM ${ARROW_INCLUDE_DIRS})
    if (TARGET arrow_shared)
        set(ARROW_SHARED_LIB arrow_shared)
    endif()
    if (TARGET arrow_static)
        set(ARROW_STATIC_LIB arrow_static)
    endif()
endif ()

# Find Doxygen
if (BUILD_DOC)
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
endif()

add_subdirectory(utils)
add_subdirectory(codegen)
add_subdirectory(storages)
add_subdirectory(engines)
add_subdirectory(bin)
if (BUILD_TEST)
   add_subdirectory(tests)
endif()


file(GLOB_RECURSE FILES_NEED_LINT
        "engines/*.cc"
        "engines/*.h"
        "bin/*.cc"
        "storages/*.h"
        "storages/*.cc"
        "test/*.h"
        "test/*.cc"
        "third_pary/*.h"
        "third_pary/*.cc" EXCEPT "*.act.h" "*.actg.h" "*.autogen.h" "*.autogen.cc")
list(FILTER FILES_NEED_LINT EXCLUDE REGEX ".*\.act.h$|.*\.actg.h$|.*\.autogen.h$|.*\.autogen.cc$")
# gsa_clformat
add_custom_target(flex_clformat
    COMMAND clang-format --style=file -i ${FILES_NEED_LINT}
    COMMENT "Running clang-format, using clang-format-8 from https://github.com/muttleyxd/clang-tools-static-binaries/releases"
    VERBATIM)

if (NOT DEFINED CPACK_PACKAGE_NAME)
  set(CPACK_PACKAGE_NAME "graphscope_flex")
endif ()
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


#install CMakeLists.txt.template to resources/
install(FILES resources/hqps/CMakeLists.txt.template DESTINATION lib/flex/)

include(CPack)

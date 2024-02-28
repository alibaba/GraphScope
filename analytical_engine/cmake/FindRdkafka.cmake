# This file is used to find librdkafka library in CMake script, modified from the
# code from
#
#   https://github.com/BVLC/caffe/blob/master/cmake/Modules/FindGlog.cmake
#
# which is licensed under the 2-Clause BSD License.
#
# - Try to find librdkafka
#
# The following variables are optionally searched for defaults
#  RDKAFKA_ROOT_DIR:            Base directory where all RDKAFKA components are found
#
# The following are set after configuration is done:
#  RDKAFKA_FOUND
#  RDKAFKA_INCLUDE_DIRS
#  RDKAFKA_LIBRARIES
#  RDKAFKA_LIBRARY_DIRS

include(FindPackageHandleStandardArgs)

set(RDKAFKA_ROOT_DIR "" CACHE PATH "Folder contains librdkafka")

# We are testing only a couple of files in the include directories
find_path(RDKAFKA_INCLUDE_DIR librdkafka PATHS ${RDKAFKA_ROOT_DIR}/include)

find_library(RDKAFKA_LIBRARY rdkafka PATHS  ${RDKAFKA_ROOT_DIR}/lib)
find_library(RDKAFKA++_LIBRARY rdkafka++ PATHS  ${RDKAFKA_ROOT_DIR}/lib)

find_package_handle_standard_args(RDKAFKA DEFAULT_MSG RDKAFKA_INCLUDE_DIR RDKAFKA_LIBRARY)


if(RDKAFKA_FOUND)
    set(RDKAFKA_INCLUDE_DIRS ${RDKAFKA_INCLUDE_DIR})
    # The RDKAFKA_LIBRARY comes later, since it is depended by the former.
    set(RDKAFKA_LIBRARIES ${RDKAFKA++_LIBRARY} ${RDKAFKA_LIBRARY})
    message(STATUS "Found rdkafka (include: ${RDKAFKA_INCLUDE_DIRS}, library: ${RDKAFKA_LIBRARIES})")
    mark_as_advanced(RDKAFKA_LIBRARY_DEBUG RDKAFKA_LIBRARY_RELEASE
                     RDKAFKA_LIBRARY RDKAFKA_INCLUDE_DIR RDKAFKA_ROOT_DIR)
endif()

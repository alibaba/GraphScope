# - Try to find DoubleConversion
#
# The following variables are optionally searched for defaults
#  DOUBLE_CONVERSION_ROOT_DIR:            Base directory where all DoubleConversion components are found
#
# The following are set after configuration is done:
#  DOUBLE_CONVERSION_FOUND
#  DOUBLE_CONVERSION_INCLUDE_DIRS
#  DOUBLE_CONVERSION_LIBRARIES
#  DOUBLE_CONVERSION_LIBRARY_DIRS

include(FindPackageHandleStandardArgs)

set(DOUBLE_CONVERSION_ROOT_DIR "" CACHE PATH "Folder contains facebook DOUBLE_CONVERSION")

# We are testing only a couple of files in the include directories
find_path(DOUBLE_CONVERSION_INCLUDE_DIR double-conversion PATHS ${DOUBLE_CONVERSION_ROOT_DIR}/include)

find_library(DOUBLE_CONVERSION_LIBRARY double-conversion PATHS  ${DOUBLE_CONVERSION_ROOT_DIR}/lib)

find_package_handle_standard_args(DOUBLE_CONVERSION DEFAULT_MSG DOUBLE_CONVERSION_INCLUDE_DIR DOUBLE_CONVERSION_LIBRARY)


if(DOUBLE_CONVERSION_FOUND)
    set(DOUBLE_CONVERSION_INCLUDE_DIRS ${DOUBLE_CONVERSION_INCLUDE_DIR})
    set(DOUBLE_CONVERSION_LIBRARIES ${DOUBLE_CONVERSION_LIBRARY})
    message(STATUS "Found DOUBLE_CONVERSION (include: ${DOUBLE_CONVERSION_INCLUDE_DIRS}, library: ${DOUBLE_CONVERSION_LIBRARIES})")
    mark_as_advanced(DOUBLE_CONVERSION_LIBRARY_DEBUG DOUBLE_CONVERSION_LIBRARY_RELEASE
            DOUBLE_CONVERSION_LIBRARY DOUBLE_CONVERSION_INCLUDE_DIR DOUBLE_CONVERSION_ROOT_DIR)
endif()
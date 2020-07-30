
# This file is used to find libunwind library in CMake script, is referred from code
# from
#
#   https://github.com/cmu-db/peloton/blob/master/cmake/Modules/FindLibunwind.cmake
#
# which has the following license:
#
#   Copyright (c) 2015-2018, Carnegie Mellon University Database Group
#
# Find the libunwind library
#
#  LIBUNWIND_FOUND       - True if libunwind was found.
#  LIBUNWIND_LIBRARIES   - The libraries needed to use libunwind
#  LIBUNWIND_INCLUDE_DIR - Location of unwind.h and libunwind.h

FIND_PATH(LIBUNWIND_INCLUDE_DIR libunwind.h)
if(NOT LIBUNWIND_INCLUDE_DIR)
    message(STATUS "failed to find libunwind.h")
elseif(NOT EXISTS "${LIBUNWIND_INCLUDE_DIR}/unwind.h")
    message(STATUS "libunwind.h was found, but unwind.h was not found in that directory.")
    SET(LIBUNWIND_INCLUDE_DIR "")
endif()

FIND_LIBRARY(LIBUNWIND_GENERIC_LIBRARY "unwind")
if(NOT LIBUNWIND_GENERIC_LIBRARY)
    MESSAGE(STATUS "failed to find unwind generic library")
endif()
SET(LIBUNWIND_LIBRARIES ${LIBUNWIND_GENERIC_LIBRARY})

# For some reason, we have to link to two libunwind shared object files:
# one arch-specific and one not.
#
# But that seems not required on mac.
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")
    SET(LIBUNWIND_ARCH "arm")
elseif (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
    SET(LIBUNWIND_ARCH "x86_64")
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^i.86$")
    SET(LIBUNWIND_ARCH "x86")
endif()

if(LIBUNWIND_ARCH)
    FIND_LIBRARY(LIBUNWIND_SPECIFIC_LIBRARY "unwind-${LIBUNWIND_ARCH}")
    if(NOT LIBUNWIND_SPECIFIC_LIBRARY)
        MESSAGE(STATUS "failed to find unwind-${LIBUNWIND_ARCH}")
    endif()
    if(LIBUNWIND_SPECIFIC_LIBRARY)
        SET(LIBUNWIND_LIBRARIES ${LIBUNWIND_LIBRARIES} ${LIBUNWIND_SPECIFIC_LIBRARY})
    else()
        if(APPLE)
            SET(LIBUNWIND_LIBRARIES ${LIBUNWIND_LIBRARIES})
        endif()
    endif()
endif(LIBUNWIND_ARCH)

MARK_AS_ADVANCED(LIBUNWIND_LIBRARIES LIBUNWIND_INCLUDE_DIR)

FIND_PACKAGE_HANDLE_STANDARD_ARGS(LibUnwind DEFAULT_MSG
        LIBUNWIND_LIBRARIES LIBUNWIND_INCLUDE_DIR)
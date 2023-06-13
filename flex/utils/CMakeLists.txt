
file(GLOB_RECURSE UTILS_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(flex_utils SHARED ${UTILS_SRC_FILES})

install(TARGETS flex_utils
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/flex/
        FILES_MATCHING
        PATTERN "*.h"
)

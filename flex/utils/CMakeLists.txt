
file(GLOB_RECURSE UTILS_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(flex_utils SHARED ${UTILS_SRC_FILES})
if (ARROW_SHARED_LIB)
        target_link_libraries(flex_utils ${ARROW_SHARED_LIB})
else()
        target_link_libraries(flex_utils ${ARROW_STATIC_LIB})
endif()
target_link_libraries(flex_utils ${YAML_CPP_LIBRARIES})

install(TARGETS flex_utils
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/flex/
        FILES_MATCHING
        PATTERN "*.h"
)

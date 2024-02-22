file(GLOB_RECURSE BSP_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
  
add_library(flex_bsp SHARED ${BSP_SRC_FILES})
target_link_libraries(flex_bsp ${LIBGRAPELITE_LIBRARIES})
install_flex_target(flex_bsp)
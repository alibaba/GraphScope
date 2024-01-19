find_package (Hiactor)
if (Hiactor_FOUND)
  include (${Hiactor_CODEGEN_CMAKE_FILE})

  hiactor_codegen (server_actor_autogen server_actor_autogen_files
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/
        INCLUDE_PATHS ${Hiactor_INCLUDE_DIR},${CMAKE_CURRENT_SOURCE_DIR}/../../../,${CMAKE_CURRENT_SOURCE_DIR}/../../third_party/nlohmann-json/single_include/)

  file(GLOB_RECURSE SERVER_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
  #if BUILD_HQPS, remove admin_http_handler.cc and hqps_*.cc
  if (NOT BUILD_HQPS)
      list(REMOVE_ITEM SERVER_FILES "${CMAKE_CURRENT_SOURCE_DIR}/handler/admin_http_handler.cc")
      list(REMOVE_ITEM SERVER_FILES "${CMAKE_CURRENT_SOURCE_DIR}/handler/hqps_http_handler.cc")
      list(REMOVE_ITEM SERVER_FILES "${CMAKE_CURRENT_SOURCE_DIR}/service/hqps_service.cc")
      list(REMOVE_ITEM SERVER_FILES "${CMAKE_CURRENT_SOURCE_DIR}/service/codegen_proxy.h")
  endif ()

  add_library(flex_server STATIC ${SERVER_FILES} ${server_actor_autogen_files})
  add_dependencies(flex_server server_actor_autogen)
  target_compile_options (flex_server
        PUBLIC
        -Wno-attributes)
  target_link_libraries(flex_server Hiactor::hiactor flex_graph_db)
  if (BUILD_HQPS)
      target_include_directories(flex_server PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../hqps/)
      target_link_libraries(flex_server hqps_plan_proto)
  else ()
      # find protobuf and link, since hqps is not built
      find_package(Protobuf REQUIRED)
      target_include_directories(flex_server PUBLIC ${PROTOBUF_INCLUDE_DIRS})
      target_link_libraries(flex_server ${PROTOBUF_LIBRARIES})
  endif ()

  install_without_export_flex_target(flex_server)
endif ()

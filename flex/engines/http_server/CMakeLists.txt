find_package (Hiactor)
if (Hiactor_FOUND)
  include (${Hiactor_CODEGEN_CMAKE_FILE})

  hiactor_codegen (server_actor_autogen server_actor_autogen_files
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/
        INCLUDE_PATHS ${Hiactor_INCLUDE_DIR},${CMAKE_CURRENT_SOURCE_DIR}/../../../,${CMAKE_CURRENT_SOURCE_DIR}/../../third_party/nlohmann-json/single_include/)

  if (NOT BUILD_HQPS)
    list(FILTER server_actor_autogen_files EXCLUDE REGEX ".*admin.*")
    list(FILTER server_actor_autogen_files EXCLUDE REGEX ".*codegen.*")
  endif ()

  if (NOT BUILD_PROXY)
    list(FILTER server_actor_autogen_files EXCLUDE REGEX ".*proxy.*")
  endif ()

  # get all .cc files in current directory, except for generated/
  file(GLOB_RECURSE SERVER_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
  list(FILTER SERVER_FILES EXCLUDE REGEX ".*generated.*")

  if (NOT BUILD_HQPS)
    list(FILTER SERVER_FILES EXCLUDE REGEX ".*admin*")
    list(FILTER SERVER_FILES EXCLUDE REGEX ".*hqps*")
    list(FILTER SERVER_FILES EXCLUDE REGEX ".*codegen*")
    list(FILTER SERVER_FILES EXCLUDE REGEX ".*workdir_manipulator*")
  endif ()

  if (NOT BUILD_PROXY)
    list(FILTER SERVER_FILES EXCLUDE REGEX ".*proxy_actor*")
    list(FILTER SERVER_FILES EXCLUDE REGEX ".*proxy_http*")
    list(FILTER SERVER_FILES EXCLUDE REGEX ".*proxy_service*")
  endif ()

  message(STATUS "SERVER_FILES: ${SERVER_FILES}")

  add_library(flex_server STATIC ${SERVER_FILES} ${server_actor_autogen_files})
  add_dependencies(flex_server server_actor_autogen)
  target_compile_options (flex_server
        PUBLIC
        -Wno-attributes)
  target_link_libraries(flex_server Hiactor::hiactor flex_graph_db)

  # Hiactor/seastar could be compiled with a compiler support option: -Wno-maybe-uninitialized, 
  # so when we are compiling flex with a compiler that does not support this option(e.g. AOCC compiler), 
  # we need to remove it from INTERFACE_COMPILE_OPTIONS.
  # Otherwise, we will got error: unrecognized warning option '-Wno-maybe-uninitialized'
  get_target_property(seastar_options Hiactor::seastar INTERFACE_COMPILE_OPTIONS)
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag("-Wno-maybe-uninitialized" COMPILER_SUPPORTS_WNO_MAYBE_UNINITIALIZED)
  if (NOT COMPILER_SUPPORTS_WNO_MAYBE_UNINITIALIZED)
    list(FILTER seastar_options EXCLUDE REGEX "-Wno-maybe-uninitialized")
    message(STATUS "remove -Wno-maybe-uninitialized from seastar_options, since not supported by ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
  endif ()
  set_target_properties(Hiactor::seastar PROPERTIES INTERFACE_COMPILE_OPTIONS "${seastar_options}")
  
  if (BUILD_HQPS)
      target_include_directories(flex_server PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../hqps/)
      target_link_libraries(flex_server hqps_plan_proto)
  endif ()
  if (OPENTELEMETRY_CPP_FOUND)
    target_link_libraries(flex_server otel)
  endif()
  target_link_libraries(flex_server flex_metadata_store)
  install_without_export_flex_target(flex_server)
endif ()

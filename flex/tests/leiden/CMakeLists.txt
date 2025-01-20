find_package(igraph 0.10 CONFIG REQUIRED)
add_library(libleidenalg STATIC
  GraphHelper.cpp
  Optimiser.cpp
  MutableVertexPartition.cpp

  ModularityVertexPartition.cpp
  SignificanceVertexPartition.cpp
  SurpriseVertexPartition.cpp

  ResolutionParameterVertexPartition.cpp  
  LinearResolutionParameterVertexPartition.cpp
  CPMVertexPartition.cpp
  RBConfigurationVertexPartition.cpp
  RBERVertexPartition.cpp
  GraphProxy.cc

)
target_link_libraries(libleidenalg
  igraph::igraph
)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Make sure we can find include files
target_include_directories(
  libleidenalg
  PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
  $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>
)


add_executable(leiden
  test.cc
)

target_link_libraries(leiden
  libleidenalg
  ${GLOG_LIBRARIES}
  flex_graph_db
)
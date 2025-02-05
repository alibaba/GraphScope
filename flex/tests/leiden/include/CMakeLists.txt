target_sources(libleidenalg
  PUBLIC
  FILE_SET headers
  TYPE HEADERS
  BASE_DIRS 
    ${PROJECT_SOURCE_DIR}/include/
  FILES 
    Optimiser.h 
    GraphHelper.h 
    MutableVertexPartition.h
    
    ModularityVertexPartition.h
    SignificanceVertexPartition.h
    SurpriseVertexPartition.h

    ResolutionParameterVertexPartition.h
    LinearResolutionParameterVertexPartition.h    
    CPMVertexPartition.h
    RBConfigurationVertexPartition.h
    RBERVertexPartition.h

  )

# OpenTelemetry
find_package(opentelemetry-cpp CONFIG REQUIRED)
include_directories("${OPENTELEMETRY_CPP_INCLUDE_DIRS}")

add_library(otel "otel.cc")
target_link_libraries(otel ${OPENTELEMETRY_CPP_LIBRARIES})

install_without_export_flex_target(otel)

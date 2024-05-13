/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/otel/otel.h"

#ifdef HAVE_OPENTELEMETRY_CPP

#include <memory>
#include <string>
#include <vector>

#include "opentelemetry/context/propagation/global_propagator.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_log_record_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h"
#include "opentelemetry/logs/provider.h"
#include "opentelemetry/metrics/provider.h"
#include "opentelemetry/sdk/logs/logger_context_factory.h"
#include "opentelemetry/sdk/logs/logger_provider_factory.h"
#include "opentelemetry/sdk/logs/simple_log_record_processor_factory.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h"
#include "opentelemetry/sdk/metrics/meter_context_factory.h"
#include "opentelemetry/sdk/metrics/meter_provider_factory.h"
#include "opentelemetry/sdk/metrics/view/view_registry_factory.h"
#include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#include "opentelemetry/sdk/trace/batch_span_processor_options.h"
#include "opentelemetry/sdk/trace/simple_processor_factory.h"
#include "opentelemetry/sdk/trace/tracer_context_factory.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/trace/propagation/http_trace_context.h"
#include "opentelemetry/trace/provider.h"

namespace otel {

namespace nostd = opentelemetry::nostd;
namespace otlp = opentelemetry::exporter::otlp;
namespace resource = opentelemetry::sdk::resource;

namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;

namespace metrics_api = opentelemetry::metrics;
namespace metrics_sdk = opentelemetry::sdk::metrics;
namespace trace_exporter = opentelemetry::exporter::trace;
namespace metrics_exporter = opentelemetry::exporter::metrics;

namespace logs_api = opentelemetry::logs;
namespace logs_sdk = opentelemetry::sdk::logs;

namespace context_api = opentelemetry::context;

// Class definition for context propagation
std::string name{"interactive-service"};
std::string version{"0.0.1"};

// ===== GENERAL SETUP =====
void initTracer() {
  resource::ResourceAttributes resource_attributes = {
      {"service.name", name}, {"service.version", version}};

  auto resource = resource::Resource::Create(resource_attributes);

  auto exporter = otlp::OtlpHttpExporterFactory::Create();
  trace_sdk::BatchSpanProcessorOptions options{};
  auto processor = trace_sdk::BatchSpanProcessorFactory::Create(
      std::move(exporter), options);
  std::vector<std::unique_ptr<trace_sdk::SpanProcessor>> processors;
  processors.push_back(std::move(processor));

  auto context =
      trace_sdk::TracerContextFactory::Create(std::move(processors), resource);
  std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
      trace_sdk::TracerProviderFactory::Create(std::move(context));
  // Set the global trace provider
  trace_api::Provider::SetTracerProvider(provider);
  // set global propagator
  context_api::propagation::GlobalTextMapPropagator::SetGlobalPropagator(
      nostd::shared_ptr<context_api::propagation::TextMapPropagator>(
          new trace_api::propagation::HttpTraceContext()));
}

// ===== METRIC SETUP =====
void initMeter() {
  resource::ResourceAttributes resource_attributes = {
      {"service.name", name}, {"service.version", version}};
  auto resource = resource::Resource::Create(resource_attributes);
  // This creates the exporter with the options we have defined above.
  auto exporter = otlp::OtlpHttpMetricExporterFactory::Create();
  metrics_sdk::PeriodicExportingMetricReaderOptions options;
  std::unique_ptr<metrics_sdk::MetricReader> reader{
      new metrics_sdk::PeriodicExportingMetricReader(std::move(exporter),
                                                     options)};
  auto context = metrics_sdk::MeterContextFactory::Create(
      metrics_sdk::ViewRegistryFactory::Create(), resource);
  context->AddMetricReader(std::move(reader));
  auto u_provider =
      metrics_sdk::MeterProviderFactory::Create(std::move(context));
  std::shared_ptr<metrics_api::MeterProvider> provider(std::move(u_provider));
  metrics_api::Provider::SetMeterProvider(provider);
}

// ===== LOG SETUP =====
void initLogger() {
  resource::ResourceAttributes resource_attributes = {
      {"service.name", name}, {"service.version", version}};
  auto resource = resource::Resource::Create(resource_attributes);
  otlp::OtlpHttpLogRecordExporterOptions loggerOptions;
  auto exporter = otlp::OtlpHttpLogRecordExporterFactory::Create(loggerOptions);
  auto processor =
      logs_sdk::SimpleLogRecordProcessorFactory::Create(std::move(exporter));
  std::vector<std::unique_ptr<logs_sdk::LogRecordProcessor>> processors;
  processors.push_back(std::move(processor));
  auto context =
      logs_sdk::LoggerContextFactory::Create(std::move(processors), resource);
  std::shared_ptr<logs_api::LoggerProvider> provider =
      logs_sdk::LoggerProviderFactory::Create(std::move(context));
  logs_api::Provider::SetLoggerProvider(provider);
}

// ===== CLEANUP =====

void cleanUpTracer() {
  std::shared_ptr<opentelemetry::trace::TracerProvider> none;
  trace_api::Provider::SetTracerProvider(none);
}

nostd::shared_ptr<logs_api::Logger> get_logger(std::string scope) {
  auto provider = logs_api::Provider::GetLoggerProvider();
  return provider->GetLogger(name + "_logger", name, OPENTELEMETRY_SDK_VERSION);
}

opentelemetry::nostd::shared_ptr<trace_api::Tracer> get_tracer(
    std::string tracer_name) {
  auto provider = trace_api::Provider::GetTracerProvider();
  return provider->GetTracer(tracer_name);
}

nostd::unique_ptr<metrics_api::Counter<uint64_t>> create_int_counter(
    std::string name, std::string version) {
  std::string counter_name = name + "_counter";
  auto provider = metrics_api::Provider::GetMeterProvider();
  nostd::shared_ptr<metrics_api::Meter> meter =
      provider->GetMeter(name, version);
  auto int_counter = meter->CreateUInt64Counter(counter_name);
  return int_counter;
}

nostd::unique_ptr<metrics_api::Histogram<double>> create_double_histogram(
    std::string name, std::string version) {
  std::string histogram_name = name + "_histogram";
  auto provider = metrics_api::Provider::GetMeterProvider();
  nostd::shared_ptr<metrics_api::Meter> meter =
      provider->GetMeter(name, version);
  auto histogram = meter->CreateDoubleHistogram(histogram_name);
  return histogram;
}

opentelemetry::trace::StartSpanOptions get_parent_ctx(
    opentelemetry::context::Context& context,
    std::map<std::string, std::string>& headers) {
  auto propagator = opentelemetry::context::propagation::
      GlobalTextMapPropagator::GetGlobalPropagator();
  otel::HttpTextMapCarrier<decltype(headers)> carrier(headers);
  auto new_context = propagator->Extract(carrier, context);

  opentelemetry::trace::StartSpanOptions options;
  options.kind = opentelemetry::trace::SpanKind::kServer;
  auto parent_ctx = opentelemetry::trace::GetSpan(new_context)->GetContext();
  options.parent = opentelemetry::trace::GetSpan(new_context)->GetContext();
  return options;
}
}  // namespace otel
#endif  // HAVE_OPENTELEMETRY_CPP

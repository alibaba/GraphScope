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

#ifndef OTEL_OTEL_H_
#define OTEL_OTEL_H_

#ifdef HAVE_OPENTELEMETRY_CPP

#include <string>

#include "opentelemetry/context/propagation/global_propagator.h"
#include "opentelemetry/exporters/ostream/log_record_exporter.h"
#include "opentelemetry/exporters/ostream/metric_exporter_factory.h"
#include "opentelemetry/exporters/ostream/span_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_log_record_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h"
#include "opentelemetry/logs/provider.h"
#include "opentelemetry/metrics/provider.h"
#include "opentelemetry/sdk/logs/logger.h"
#include "opentelemetry/sdk/logs/logger_context_factory.h"
#include "opentelemetry/sdk/logs/logger_provider_factory.h"
#include "opentelemetry/sdk/logs/simple_log_record_processor_factory.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h"
#include "opentelemetry/sdk/metrics/meter_context_factory.h"
#include "opentelemetry/sdk/metrics/meter_provider_factory.h"
#include "opentelemetry/sdk/metrics/view/view_registry_factory.h"
#include "opentelemetry/sdk/trace/simple_processor_factory.h"
#include "opentelemetry/sdk/trace/tracer_context.h"
#include "opentelemetry/sdk/trace/tracer_context_factory.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/trace/propagation/http_trace_context.h"
#include "opentelemetry/trace/provider.h"

namespace otel {
// Class definition for context propagation
namespace nostd = opentelemetry::nostd;

namespace trace_api = opentelemetry::trace;
namespace metrics_api = opentelemetry::metrics;
namespace logs_api = opentelemetry::logs;

template <typename T>
class HttpTextMapCarrier
    : public opentelemetry::context::propagation::TextMapCarrier {
 public:
  HttpTextMapCarrier(T& headers) : headers_(headers) {}
  HttpTextMapCarrier() = default;
  opentelemetry::nostd::string_view Get(
      opentelemetry::nostd::string_view key) const noexcept override {
    auto it = headers_.find(key.data());
    if (it != headers_.end()) {
      return opentelemetry::nostd::string_view(it->second);
    }
    return "";
  }

  void Set(opentelemetry::nostd::string_view key,
           opentelemetry::nostd::string_view value) noexcept override {
    headers_.insert(std::pair{std::string(key), std::string(value)});
  }

  T headers_;
};

// ===== GENERAL SETUP =====
void initTracer();

// ===== METRIC SETUP =====
void initMeter();

// ===== LOG SETUP =====
void initLogger();

// ===== CLEANUP =====
void cleanUpTracer();

nostd::shared_ptr<logs_api::Logger> get_logger(std::string scope);

opentelemetry::nostd::shared_ptr<trace_api::Tracer> get_tracer(
    std::string tracer_name);

nostd::unique_ptr<metrics_api::Counter<uint64_t>> create_int_counter(
    std::string name, std::string version = "");

nostd::unique_ptr<metrics_api::Histogram<double>> create_double_histogram(
    std::string name, std::string version = "");

opentelemetry::trace::StartSpanOptions get_parent_ctx(
    opentelemetry::context::Context& context,
    std::map<std::string, std::string>& headers);

}  // namespace otel
#endif  // OTEL_OTEL_H_

#endif  // HAVE_OPENTELEMETRY_CPP
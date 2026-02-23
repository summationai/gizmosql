// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/tracer.h>
#endif

namespace gizmosql {

enum class OtlpExporterType {
  kNone,
  kHttp,
};

struct TelemetryConfig {
  bool enabled = false;
  OtlpExporterType exporter_type = OtlpExporterType::kHttp;

  std::string endpoint;
  std::string service_name = "gizmosql";
  std::string service_version;
  std::string deployment_environment;
  std::string headers;

  std::chrono::milliseconds export_interval{5000};
  std::chrono::milliseconds export_timeout{30000};

  bool traces_enabled = true;
  bool metrics_enabled = true;
};

void InitTelemetry(const TelemetryConfig& config);
void ShutdownTelemetry();
bool IsTelemetryEnabled() noexcept;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> GetTracer();
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Meter> GetMeter();

class ScopedSpan {
 public:
  explicit ScopedSpan(const std::string& name);
  ~ScopedSpan();

  ScopedSpan(const ScopedSpan&) = delete;
  ScopedSpan& operator=(const ScopedSpan&) = delete;
  ScopedSpan(ScopedSpan&&) = delete;
  ScopedSpan& operator=(ScopedSpan&&) = delete;

  void SetAttribute(const std::string& key, const std::string& value);
  void SetAttribute(const std::string& key, int64_t value);
  void SetAttribute(const std::string& key, double value);
  void SetAttribute(const std::string& key, bool value);
  void RecordError(const std::string& error_message);
  void SetStatus(bool success, const std::string& description = "");

  opentelemetry::trace::Span& GetSpan();
  opentelemetry::trace::SpanContext GetContext() const;

 private:
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
  opentelemetry::trace::Scope scope_;
};
#endif

namespace metrics {
void RecordRpcCall(const std::string& method, const std::string& status,
                   double duration_ms);
void RecordQueryExecution(const std::string& operation, const std::string& status,
                          double duration_ms);
void RecordActiveConnections(int64_t count);
void RecordBytesTransferred(const std::string& direction, int64_t bytes);
}  // namespace metrics

OtlpExporterType ParseExporterType(const std::string& type_str);
std::string GetDefaultEndpoint(OtlpExporterType type);

}  // namespace gizmosql

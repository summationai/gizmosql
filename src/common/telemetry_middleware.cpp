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

#include "telemetry_middleware.h"

#include "gizmosql_telemetry.h"

#include <algorithm>
#include <array>
#include <arrow/flight/server.h>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/trace/context.h>
#include <opentelemetry/trace/default_span.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/span_context.h>
#include <opentelemetry/trace/tracer.h>

namespace context_api = opentelemetry::context;
namespace context_propagation_api = opentelemetry::context::propagation;
namespace trace_api = opentelemetry::trace;
#endif

namespace gizmosql {

static bool EqualsIgnoreCase(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(), right.end(),
                    [](char l, char r) {
                      return std::tolower(static_cast<unsigned char>(l)) ==
                             std::tolower(static_cast<unsigned char>(r));
                    });
}

static bool HasHeaderIgnoreCase(const flight::CallHeaders& incoming_headers,
                                std::string_view key) {
  for (auto iter = incoming_headers.begin(); iter != incoming_headers.end(); ++iter) {
    if (EqualsIgnoreCase(iter->first, key)) {
      return true;
    }
  }
  return false;
}

static std::optional<std::string> GetHeaderIgnoreCase(
    const flight::CallHeaders& incoming_headers, std::string_view key) {
  for (auto iter = incoming_headers.begin(); iter != incoming_headers.end(); ++iter) {
    if (EqualsIgnoreCase(iter->first, key)) {
      return std::string(iter->second);
    }
  }
  return std::nullopt;
}

static bool ParseUnsigned(std::string_view value, int base, uint64_t* out) {
  if (value.empty() || out == nullptr) {
    return false;
  }

  uint64_t parsed = 0;
  const char* begin = value.data();
  const char* end = value.data() + value.size();
  const auto [ptr, ec] = std::from_chars(begin, end, parsed, base);
  if (ec != std::errc() || ptr != end) {
    return false;
  }

  *out = parsed;
  return true;
}

static bool ParseSigned(std::string_view value, int base, int64_t* out) {
  if (value.empty() || out == nullptr) {
    return false;
  }

  int64_t parsed = 0;
  const char* begin = value.data();
  const char* end = value.data() + value.size();
  const auto [ptr, ec] = std::from_chars(begin, end, parsed, base);
  if (ec != std::errc() || ptr != end) {
    return false;
  }

  *out = parsed;
  return true;
}

static uint64_t ParseDatadogTraceHighBits(std::string_view tags_header) {
  static constexpr std::string_view kTidKey = "_dd.p.tid=";
  const size_t start = tags_header.find(kTidKey);
  if (start == std::string_view::npos) {
    return 0;
  }

  size_t pos = start + kTidKey.size();
  size_t end = pos;
  while (end < tags_header.size() &&
         std::isxdigit(static_cast<unsigned char>(tags_header[end]))) {
    ++end;
  }
  if (end == pos) {
    return 0;
  }

  uint64_t high_bits = 0;
  if (!ParseUnsigned(tags_header.substr(pos, end - pos), 16, &high_bits)) {
    return 0;
  }

  return high_bits;
}

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
static std::optional<context_api::Context> BuildDatadogParentContext(
    const flight::CallHeaders& incoming_headers,
    const context_api::Context& current_context) {
  const auto trace_id_header =
      GetHeaderIgnoreCase(incoming_headers, "x-datadog-trace-id");
  const auto parent_id_header =
      GetHeaderIgnoreCase(incoming_headers, "x-datadog-parent-id");
  if (!trace_id_header.has_value() || !parent_id_header.has_value()) {
    return std::nullopt;
  }

  uint64_t trace_id_low = 0;
  uint64_t parent_span_id = 0;
  if (!ParseUnsigned(*trace_id_header, 10, &trace_id_low) ||
      !ParseUnsigned(*parent_id_header, 10, &parent_span_id) || trace_id_low == 0 ||
      parent_span_id == 0) {
    return std::nullopt;
  }

  uint64_t trace_id_high = 0;
  if (const auto tags = GetHeaderIgnoreCase(incoming_headers, "x-datadog-tags");
      tags.has_value()) {
    trace_id_high = ParseDatadogTraceHighBits(*tags);
  }

  std::array<uint8_t, trace_api::TraceId::kSize> trace_id_bytes{};
  for (int idx = 0; idx < 8; ++idx) {
    trace_id_bytes[idx] = static_cast<uint8_t>(trace_id_high >> (56 - idx * 8));
    trace_id_bytes[8 + idx] = static_cast<uint8_t>(trace_id_low >> (56 - idx * 8));
  }

  std::array<uint8_t, trace_api::SpanId::kSize> parent_span_bytes{};
  for (int idx = 0; idx < 8; ++idx) {
    parent_span_bytes[idx] = static_cast<uint8_t>(parent_span_id >> (56 - idx * 8));
  }

  bool sampled = true;
  if (const auto sampling_priority =
          GetHeaderIgnoreCase(incoming_headers, "x-datadog-sampling-priority");
      sampling_priority.has_value()) {
    int64_t priority = 0;
    if (ParseSigned(*sampling_priority, 10, &priority)) {
      sampled = priority > 0;
    }
  }

  const auto trace_id_span = opentelemetry::nostd::span<const uint8_t, trace_api::TraceId::kSize>(
      trace_id_bytes);
  const auto parent_span_span =
      opentelemetry::nostd::span<const uint8_t, trace_api::SpanId::kSize>(
          parent_span_bytes);
  const trace_api::SpanContext span_context(
      trace_api::TraceId(trace_id_span), trace_api::SpanId(parent_span_span),
      trace_api::TraceFlags(static_cast<uint8_t>(
          sampled ? trace_api::TraceFlags::kIsSampled : 0)),
      true);
  if (!span_context.IsValid()) {
    return std::nullopt;
  }

  auto parent_span = opentelemetry::nostd::shared_ptr<trace_api::Span>(
      new trace_api::DefaultSpan(span_context));
  auto parent_context = trace_api::SetSpan(current_context, parent_span);
  return parent_context;
}
#endif

static const char* FlightMethodName(flight::FlightMethod method) {
  switch (method) {
    case flight::FlightMethod::Handshake:
      return "Handshake";
    case flight::FlightMethod::ListFlights:
      return "ListFlights";
    case flight::FlightMethod::GetFlightInfo:
      return "GetFlightInfo";
    case flight::FlightMethod::GetSchema:
      return "GetSchema";
    case flight::FlightMethod::DoGet:
      return "DoGet";
    case flight::FlightMethod::DoPut:
      return "DoPut";
    case flight::FlightMethod::DoAction:
      return "DoAction";
    case flight::FlightMethod::ListActions:
      return "ListActions";
    case flight::FlightMethod::DoExchange:
      return "DoExchange";
    case flight::FlightMethod::PollFlightInfo:
      return "PollFlightInfo";
    default:
      return "Unknown";
  }
}

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
class FlightCallHeadersCarrier final : public context_propagation_api::TextMapCarrier {
 public:
  explicit FlightCallHeadersCarrier(const flight::CallHeaders& incoming_headers)
      : incoming_headers_(incoming_headers) {}

  opentelemetry::nostd::string_view Get(
      opentelemetry::nostd::string_view key) const noexcept override {
    const std::string key_str(key.data(), key.size());
    auto iter = incoming_headers_.find(key_str);
    if (iter != incoming_headers_.end()) {
      cached_value_ = std::string(iter->second);
      return cached_value_;
    }

    for (auto header_iter = incoming_headers_.begin(); header_iter != incoming_headers_.end();
         ++header_iter) {
      if (gizmosql::EqualsIgnoreCase(header_iter->first, key_str)) {
        cached_value_ = std::string(header_iter->second);
        return cached_value_;
      }
    }

    cached_value_.clear();
    return {};
  }

  void Set(opentelemetry::nostd::string_view /*key*/,
           opentelemetry::nostd::string_view /*value*/) noexcept override {}

 private:
  const flight::CallHeaders& incoming_headers_;
  mutable std::string cached_value_;
};

class TelemetrySpanScope {
 public:
  explicit TelemetrySpanScope(const opentelemetry::nostd::shared_ptr<trace_api::Span>& span)
      : scope_(span) {}

 private:
  trace_api::Scope scope_;
};

struct TelemetryMiddleware::SpanHolder {
  explicit SpanHolder(opentelemetry::nostd::shared_ptr<trace_api::Span> input_span)
      : span(std::move(input_span)), scope(std::make_unique<trace_api::Scope>(span)) {}

  ~SpanHolder() {
    if (span) {
      span->End();
    }
  }

  opentelemetry::nostd::shared_ptr<trace_api::Span> span;
  std::unique_ptr<trace_api::Scope> scope;
};
#else
struct TelemetryMiddleware::SpanHolder {};

class TelemetrySpanScope {
 public:
  TelemetrySpanScope() = default;
};
#endif

TelemetryMiddleware::TelemetryMiddleware(flight::FlightMethod method, std::string peer,
                                         const flight::CallHeaders& incoming_headers)
    : method_(method),
      peer_(std::move(peer)),
      start_time_(std::chrono::steady_clock::now()) {
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (!IsTelemetryEnabled()) {
    return;
  }

  auto tracer = GetTracer();
  trace_api::StartSpanOptions span_options;
  span_options.kind = trace_api::SpanKind::kServer;

  FlightCallHeadersCarrier carrier(incoming_headers);
  auto current_context = context_api::RuntimeContext::GetCurrent();
  auto propagator = context_propagation_api::GlobalTextMapPropagator::GetGlobalPropagator();
  auto parent_context = propagator ? propagator->Extract(carrier, current_context)
                                   : current_context;
  auto parent_span_context = trace_api::GetSpan(parent_context)->GetContext();
  const bool tracecontext_parent_present = parent_span_context.IsValid();

  bool datadog_parent_present = false;
  if (!tracecontext_parent_present) {
    if (auto datadog_context = BuildDatadogParentContext(incoming_headers, current_context);
        datadog_context.has_value()) {
      parent_context = std::move(*datadog_context);
      parent_span_context = trace_api::GetSpan(parent_context)->GetContext();
      datadog_parent_present = parent_span_context.IsValid();
    }
  }

  const bool has_parent_context = parent_span_context.IsValid();
  auto parent_context_token = context_api::RuntimeContext::Attach(parent_context);
  (void)parent_context_token;
  auto span = tracer->StartSpan(std::string("gizmosql.") + FlightMethodName(method_), {},
                                span_options);

  span->SetAttribute("rpc.system", "grpc");
  span->SetAttribute("rpc.service", "arrow.flight.protocol.FlightService");
  span->SetAttribute("rpc.method", FlightMethodName(method_));
  span->SetAttribute("gizmosql.trace.parent_present", has_parent_context);
  span->SetAttribute("gizmosql.trace.tracecontext_parent_present",
                     tracecontext_parent_present);
  span->SetAttribute("gizmosql.trace.parent_format",
                     datadog_parent_present
                         ? "datadog"
                         : (tracecontext_parent_present ? "tracecontext" : "none"));
  span->SetAttribute("gizmosql.trace.traceparent_present",
                     HasHeaderIgnoreCase(incoming_headers, "traceparent"));
  span->SetAttribute("gizmosql.trace.datadog_parent_present",
                     HasHeaderIgnoreCase(incoming_headers, "x-datadog-parent-id"));
  span->SetAttribute("gizmosql.trace.datadog_trace_present",
                     HasHeaderIgnoreCase(incoming_headers, "x-datadog-trace-id"));
  span->SetAttribute("gizmosql.trace.datadog_context_extracted",
                     datadog_parent_present);

  if (const char* service_version = std::getenv("GIZMOSQL_OTEL_SERVICE_VERSION");
      service_version != nullptr && service_version[0] != '\0') {
    span->SetAttribute("service.version", service_version);
  }

  if (!peer_.empty()) {
    span->SetAttribute("net.peer.name", peer_);
  }

  span_holder_ = std::make_unique<SpanHolder>(std::move(span));
#endif
}

TelemetryMiddleware::~TelemetryMiddleware() = default;

std::shared_ptr<TelemetrySpanScope> TelemetryMiddleware::ActivateSpanForCurrentThread() const {
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (span_holder_ && span_holder_->span) {
    return std::make_shared<TelemetrySpanScope>(span_holder_->span);
  }
#endif
  return nullptr;
}

void TelemetryMiddleware::SendingHeaders(flight::AddCallHeaders* /*outgoing_headers*/) {}

void TelemetryMiddleware::CallCompleted(const arrow::Status& status) {
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - start_time_)
                              .count();
  const std::string status_label = status.ok() ? "OK" : status.CodeAsString();
  metrics::RecordRpcCall(FlightMethodName(method_), status_label,
                         static_cast<double>(elapsed_ms));

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (span_holder_ && span_holder_->span) {
    span_holder_->span->SetAttribute("duration_ms", static_cast<int64_t>(elapsed_ms));
    if (status.ok()) {
      span_holder_->span->SetStatus(trace_api::StatusCode::kOk);
      span_holder_->span->SetAttribute("rpc.grpc.status_code", 0);
      return;
    }

    span_holder_->span->SetStatus(trace_api::StatusCode::kError, status.ToString());
    span_holder_->span->AddEvent("error", {{"exception.type", status.CodeAsString()},
                                           {"exception.message", status.message()}});

    int grpc_code = 2;
    switch (status.code()) {
      case arrow::StatusCode::Invalid:
      case arrow::StatusCode::TypeError:
      case arrow::StatusCode::SerializationError:
        grpc_code = 3;
        break;
      case arrow::StatusCode::KeyError:
      case arrow::StatusCode::IndexError:
        grpc_code = 5;
        break;
      case arrow::StatusCode::AlreadyExists:
        grpc_code = 6;
        break;
      case arrow::StatusCode::OutOfMemory:
      case arrow::StatusCode::CapacityError:
        grpc_code = 8;
        break;
      case arrow::StatusCode::Cancelled:
        grpc_code = 1;
        break;
      case arrow::StatusCode::NotImplemented:
        grpc_code = 12;
        break;
      case arrow::StatusCode::IOError:
        grpc_code = 14;
        break;
      case arrow::StatusCode::UnknownError:
      default:
        grpc_code = 2;
        break;
    }
    span_holder_->span->SetAttribute("rpc.grpc.status_code", grpc_code);
  }
#else
  (void)status;
#endif
}

arrow::Status TelemetryMiddlewareFactory::StartCall(
    const flight::CallInfo& info, const flight::ServerCallContext& ctx,
    std::shared_ptr<flight::ServerMiddleware>* out) {
  *out = std::make_shared<TelemetryMiddleware>(info.method, ctx.peer(), ctx.incoming_headers());
  return arrow::Status::OK();
}

std::shared_ptr<TelemetrySpanScope> ActivateTelemetrySpan(
    const flight::ServerCallContext& ctx) {
  auto* middleware = ctx.GetMiddleware("telemetry");
  if (!middleware) {
    return nullptr;
  }

  auto* telemetry_middleware = dynamic_cast<TelemetryMiddleware*>(middleware);
  if (!telemetry_middleware) {
    return nullptr;
  }
  return telemetry_middleware->ActivateSpanForCurrentThread();
}

}  // namespace gizmosql

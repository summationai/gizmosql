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

#include <arrow/flight/server.h>
#include <utility>

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/tracer.h>

namespace trace_api = opentelemetry::trace;
#endif

namespace gizmosql {

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
#endif

TelemetryMiddleware::TelemetryMiddleware(flight::FlightMethod method, std::string peer)
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
  auto span = tracer->StartSpan(std::string("gizmosql.") + FlightMethodName(method_), {},
                                span_options);

  span->SetAttribute("rpc.system", "grpc");
  span->SetAttribute("rpc.service", "arrow.flight.protocol.FlightService");
  span->SetAttribute("rpc.method", FlightMethodName(method_));
  if (!peer_.empty()) {
    span->SetAttribute("net.peer.name", peer_);
  }

  span_holder_ = std::make_unique<SpanHolder>(std::move(span));
#endif
}

TelemetryMiddleware::~TelemetryMiddleware() = default;

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
  *out = std::make_shared<TelemetryMiddleware>(info.method, ctx.peer());
  return arrow::Status::OK();
}

}  // namespace gizmosql

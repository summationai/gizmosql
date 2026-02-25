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

#include <arrow/flight/server_middleware.h>
#include <chrono>
#include <memory>
#include <string>

#include "flight_sql_fwd.h"

namespace gizmosql {

class TelemetryMiddleware : public flight::ServerMiddleware {
 public:
  TelemetryMiddleware(flight::FlightMethod method, std::string peer,
                      const flight::CallHeaders& incoming_headers);
  ~TelemetryMiddleware() override;

  void SendingHeaders(flight::AddCallHeaders* outgoing_headers) override;
  void CallCompleted(const arrow::Status& status) override;
  std::string name() const override { return "telemetry"; }

 private:
  flight::FlightMethod method_;
  std::string peer_;
  std::chrono::steady_clock::time_point start_time_;

  struct SpanHolder;
  std::unique_ptr<SpanHolder> span_holder_;
};

class TelemetryMiddlewareFactory : public flight::ServerMiddlewareFactory {
 public:
  TelemetryMiddlewareFactory() = default;

  arrow::Status StartCall(const flight::CallInfo& info,
                          const flight::ServerCallContext& ctx,
                          std::shared_ptr<flight::ServerMiddleware>* out) override;
};

}  // namespace gizmosql

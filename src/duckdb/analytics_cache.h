// Licensed under the Apache License, Version 2.0.
#pragma once

#include <filesystem>
#include <memory>
#include <string>

#include <arrow/api.h>
#include <duckdb.hpp>

namespace gizmosql {
struct ClientSession;
}

namespace gizmosql::ddb {

class AnalyticsCache {
 public:
  static arrow::Result<std::unique_ptr<AnalyticsCache>> Open(
      const std::string& config_path);

  // Ticket proof binds the advertised schema to this statement and authenticated session.
  std::string TicketProof(const ClientSession& session, const std::string& sql,
                          const arrow::Schema& schema) const;

  std::shared_ptr<arrow::RecordBatchReader> TryRead(
      const ClientSession& session, const std::string& sql,
      const std::string& ticket_proof, const std::shared_ptr<arrow::Schema>& schema,
      duckdb::PreparedStatement& statement) const;

 private:
  AnalyticsCache(std::filesystem::path directory, std::string catalog, std::string epoch,
                 std::string username);
  bool EligibleSession(const ClientSession& session) const;

  std::filesystem::path directory_;
  std::string catalog_;
  std::string epoch_;
  std::string username_;
};

}  // namespace gizmosql::ddb

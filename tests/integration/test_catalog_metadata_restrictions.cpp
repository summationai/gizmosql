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

#include <gtest/gtest.h>

#include <chrono>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <jwt-cpp/jwt.h>
#include <picojson.h>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

bool HasEnterpriseLicense() {
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  return license_file != nullptr && license_file[0] != '\0';
}

#define SKIP_WITHOUT_LICENSE()                                                        \
  if (!HasEnterpriseLicense()) {                                                      \
    GTEST_SKIP() << "Catalog permissions is an enterprise feature. "                  \
                 << "Set GIZMOSQL_LICENSE_KEY_FILE environment variable.";            \
  }

const std::string kTestSecretKey = "test_secret_key_for_testing";
const std::string kServerJWTIssuer = "gizmosql";
const std::string kAllowedCatalog = "allowed_catalog";
const std::string kBlockedCatalog = "blocked_catalog";

std::string GenerateTestUUID() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 15);
  static std::uniform_int_distribution<> dis2(8, 11);

  std::stringstream ss;
  ss << std::hex;
  for (int i = 0; i < 8; i++) ss << dis(gen);
  ss << "-";
  for (int i = 0; i < 4; i++) ss << dis(gen);
  ss << "-4";
  for (int i = 0; i < 3; i++) ss << dis(gen);
  ss << "-";
  ss << dis2(gen);
  for (int i = 0; i < 3; i++) ss << dis(gen);
  ss << "-";
  for (int i = 0; i < 12; i++) ss << dis(gen);
  return ss.str();
}

std::string CreateTestJWT(const std::string& username, const std::string& role,
                          const std::string& catalog_access_json) {
  auto builder = jwt::create()
                     .set_issuer(kServerJWTIssuer)
                     .set_type("JWT")
                     .set_id("test-" + GenerateTestUUID())
                     .set_issued_at(std::chrono::system_clock::now())
                     .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours{24})
                     .set_payload_claim("sub", jwt::claim(username))
                     .set_payload_claim("role", jwt::claim(role))
                     .set_payload_claim("auth_method", jwt::claim(std::string("TestToken")))
                     .set_payload_claim("session_id", jwt::claim(GenerateTestUUID()));

  picojson::value v;
  std::string err = picojson::parse(v, catalog_access_json);
  if (err.empty()) {
    builder = builder.set_payload_claim("catalog_access", jwt::claim(v));
  }

  return builder.sign(jwt::algorithm::hs256{kTestSecretKey});
}

arrow::Result<std::shared_ptr<arrow::Table>> CollectResults(
    FlightSqlClient& client, const arrow::flight::FlightCallOptions& call_options,
    const std::unique_ptr<arrow::flight::FlightInfo>& info) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
  std::shared_ptr<arrow::Schema> schema;

  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto stream, client.DoGet(call_options, endpoint.ticket));
    ARROW_ASSIGN_OR_RAISE(auto stream_schema, stream->GetSchema());
    if (!schema) {
      schema = stream_schema;
    }

    while (true) {
      ARROW_ASSIGN_OR_RAISE(auto chunk, stream->Next());
      if (chunk.data == nullptr) {
        break;
      }
      all_batches.push_back(chunk.data);
    }
  }

  if (!schema) {
    return arrow::Status::Invalid("No schema returned from query");
  }

  if (all_batches.empty()) {
    return arrow::Table::MakeEmpty(schema);
  }

  return arrow::Table::FromRecordBatches(schema, all_batches);
}

arrow::Status ExecuteAndConsume(FlightSqlClient& client,
                                const arrow::flight::FlightCallOptions& call_options,
                                const std::string& query) {
  ARROW_ASSIGN_OR_RAISE(auto info, client.Execute(call_options, query));
  ARROW_RETURN_NOT_OK(CollectResults(client, call_options, info).status());
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Table>> ExecuteToTable(
    FlightSqlClient& client, const arrow::flight::FlightCallOptions& call_options,
    const std::string& query) {
  ARROW_ASSIGN_OR_RAISE(auto info, client.Execute(call_options, query));
  return CollectResults(client, call_options, info);
}

std::vector<std::string> GetStringColumnValues(const std::shared_ptr<arrow::Table>& table,
                                               const std::string& column_name) {
  std::vector<std::string> values;
  auto column = table->GetColumnByName(column_name);
  if (!column) {
    return values;
  }

  for (const auto& chunk : column->chunks()) {
    if (chunk->type_id() == arrow::Type::STRING) {
      auto string_array = std::static_pointer_cast<arrow::StringArray>(chunk);
      for (int64_t i = 0; i < string_array->length(); ++i) {
        if (!string_array->IsNull(i)) {
          values.emplace_back(string_array->GetString(i));
        }
      }
    } else if (chunk->type_id() == arrow::Type::LARGE_STRING) {
      auto string_array = std::static_pointer_cast<arrow::LargeStringArray>(chunk);
      for (int64_t i = 0; i < string_array->length(); ++i) {
        if (!string_array->IsNull(i)) {
          values.emplace_back(string_array->GetString(i));
        }
      }
    }
  }

  return values;
}

class CatalogMetadataRestrictionFixture
    : public gizmosql::testing::ServerTestFixture<CatalogMetadataRestrictionFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "catalog_metadata_restrictions_test.db",
        .port = 31365,
        .health_port = 31366,
        .username = "tester",
        .password = "tester",
        .init_sql_commands =
            "ATTACH ':memory:' AS allowed_catalog;"
            "ATTACH ':memory:' AS blocked_catalog;"
            "CREATE SCHEMA allowed_catalog.analytics;"
            "CREATE SCHEMA blocked_catalog.analytics;"
            "CREATE TABLE allowed_catalog.main.allowed_table (id INTEGER);"
            "CREATE TABLE allowed_catalog.analytics.allowed_metrics (id INTEGER);"
            "CREATE TABLE blocked_catalog.main.blocked_table (id INTEGER);"
            "CREATE TABLE blocked_catalog.analytics.blocked_metrics (id INTEGER);",
    };
  }

 protected:
  arrow::Result<std::unique_ptr<FlightSqlClient>> CreateClientWithToken(
      const std::string& token) {
    arrow::flight::FlightClientOptions options;
    ARROW_ASSIGN_OR_RAISE(auto location,
                          arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ARROW_ASSIGN_OR_RAISE(auto client,
                          arrow::flight::FlightClient::Connect(location, options));
    return std::make_unique<FlightSqlClient>(std::move(client));
  }

  arrow::flight::FlightCallOptions GetCallOptionsWithToken(const std::string& token) {
    arrow::flight::FlightCallOptions call_options;
    call_options.headers.push_back({"authorization", "Bearer " + token});
    return call_options;
  }

  std::string CreateRestrictedToken() {
    return CreateTestJWT("metadata_user", "user",
                         R"([{"catalog": "allowed_catalog", "access": "read"}])");
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<CatalogMetadataRestrictionFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<CatalogMetadataRestrictionFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<CatalogMetadataRestrictionFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<CatalogMetadataRestrictionFixture>::config_{};

TEST_F(CatalogMetadataRestrictionFixture, GetCatalogsReturnsOnlyAllowedCatalog) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string token = CreateRestrictedToken();
  auto call_options = GetCallOptionsWithToken(token);
  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  ASSERT_ARROW_OK_AND_ASSIGN(auto info, client->GetCatalogs(call_options));
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, CollectResults(*client, call_options, info));

  auto catalogs = GetStringColumnValues(table, "catalog_name");
  ASSERT_EQ(catalogs.size(), 1);
  EXPECT_EQ(catalogs[0], kAllowedCatalog);
}

TEST_F(CatalogMetadataRestrictionFixture, GetDbSchemasDeniesForbiddenCatalog) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string token = CreateRestrictedToken();
  auto call_options = GetCallOptionsWithToken(token);
  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  std::string blocked_catalog = kBlockedCatalog;
  auto result = client->GetDbSchemas(call_options, &blocked_catalog, nullptr);
  ASSERT_FALSE(result.ok());
  ASSERT_NE(result.status().ToString().find("Access denied"), std::string::npos);
}

TEST_F(CatalogMetadataRestrictionFixture, GetTablesDeniesForbiddenCatalog) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string token = CreateRestrictedToken();
  auto call_options = GetCallOptionsWithToken(token);
  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  std::string blocked_catalog = kBlockedCatalog;
  auto result = client->GetTables(call_options, &blocked_catalog, nullptr, nullptr, false,
                                  nullptr);
  ASSERT_FALSE(result.ok());
  ASSERT_NE(result.status().ToString().find("Access denied"), std::string::npos);
}

TEST_F(CatalogMetadataRestrictionFixture, GetTablesWildcardCatalogPatternShowsOnlyAllowedCatalog) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string token = CreateRestrictedToken();
  auto call_options = GetCallOptionsWithToken(token);
  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  std::string wildcard_catalog = "%";
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      client->GetTables(call_options, &wildcard_catalog, nullptr, nullptr, false, nullptr));
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, CollectResults(*client, call_options, info));

  auto catalogs = GetStringColumnValues(table, "catalog_name");
  ASSERT_FALSE(catalogs.empty());
  for (const auto& catalog : catalogs) {
    EXPECT_EQ(catalog, kAllowedCatalog);
  }
}

TEST_F(CatalogMetadataRestrictionFixture, UseForbiddenCatalogIsDenied) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string token = CreateRestrictedToken();
  auto call_options = GetCallOptionsWithToken(token);
  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  auto result = client->Execute(call_options, "USE blocked_catalog");
  ASSERT_FALSE(result.ok());
  ASSERT_NE(result.status().ToString().find("Access denied"), std::string::npos);
}

TEST_F(CatalogMetadataRestrictionFixture, InformationSchemaTablesShowsOnlyAllowedCatalog) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string token = CreateRestrictedToken();
  auto call_options = GetCallOptionsWithToken(token);
  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "USE allowed_catalog"));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteToTable(*client, call_options,
                     "SELECT DISTINCT table_catalog FROM information_schema.tables "
                     "ORDER BY table_catalog"));

  auto catalogs = GetStringColumnValues(table, "table_catalog");
  ASSERT_EQ(catalogs.size(), 1);
  EXPECT_EQ(catalogs[0], kAllowedCatalog);
}

TEST_F(CatalogMetadataRestrictionFixture, InformationSchemaSchemataShowsOnlyAllowedCatalog) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string token = CreateRestrictedToken();
  auto call_options = GetCallOptionsWithToken(token);
  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteToTable(*client, call_options,
                     "SELECT DISTINCT catalog_name FROM information_schema.schemata "
                     "ORDER BY catalog_name"));

  auto catalogs = GetStringColumnValues(table, "catalog_name");
  ASSERT_EQ(catalogs.size(), 1);
  EXPECT_EQ(catalogs[0], kAllowedCatalog);
}

TEST_F(CatalogMetadataRestrictionFixture, MetadataQueryWithoutCatalogColumnsIsDenied) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string token = CreateRestrictedToken();
  auto call_options = GetCallOptionsWithToken(token);
  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  auto result = client->Execute(
      call_options,
      "SELECT table_name FROM information_schema.tables ORDER BY table_name");
  ASSERT_FALSE(result.ok());
  ASSERT_NE(result.status().ToString().find("must include catalog columns"),
            std::string::npos);
}

}  // namespace

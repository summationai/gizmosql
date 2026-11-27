// Licensed under the Apache License, Version 2.0.
#include "analytics_cache.h"

#include <fstream>
#include <regex>
#include <utility>

#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types/blob.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <nlohmann/json.hpp>
#include <openssl/crypto.h>

#include "detail/gizmosql_security.h"
#include "detail/session_context.h"

namespace gizmosql::ddb {
namespace {
namespace fs = std::filesystem;
using nlohmann::json;

constexpr int64_t kMaxArtifactBytes = 8 * 1024 * 1024;
constexpr size_t kMaxMetadataBytes = 16 * 1024;
constexpr size_t kKeyBytes = 32;
const std::string kSeparator(1, '\0');
const std::string kTicketPrefix = "mv1_";

std::string ReadSmallFile(const fs::path& path, size_t limit) {
  std::ifstream file(path, std::ios::binary);
  if (!file) return {};
  std::string contents(limit + 1, '\0');
  file.read(contents.data(), static_cast<std::streamsize>(contents.size()));
  if (file.bad() || static_cast<size_t>(file.gcount()) > limit) return {};
  contents.resize(static_cast<size_t>(file.gcount()));
  return contents;
}

bool PrivateFile(duckdb::Connection& connection, const fs::path& path) {
  std::error_code error;
  const auto canonical = fs::canonical(path, error);
  if (error || canonical != path || !fs::is_regular_file(path, error)) return false;
  return !duckdb::DBConfig::GetConfig(*connection.context)
              .CanAccessFile(canonical.string(), duckdb::FileType::FILE_TYPE_REGULAR);
}

bool CanUseSourceSnapshot(duckdb::Connection& connection, const std::string& settings) {
  bool eligible = false;
  connection.context->RunFunctionInTransaction([&] {
    if (!connection.IsAutoCommit()) return;
    // Use the same search path as DuckDB's function binder. A temporary macro can
    // change the query even when external access and configuration are locked.
    static const std::string aggregates[] = {"count", "sum", "min", "max"};
    for (const auto& name : aggregates) {
      const duckdb::EntryLookupInfo lookup(duckdb::CatalogType::SCALAR_FUNCTION_ENTRY,
                                           name);
      auto entry = duckdb::Catalog::GetEntry(*connection.context, "", "", lookup,
                                             duckdb::OnEntryNotFound::RETURN_NULL);
      if (!entry || entry->type != duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY ||
          !entry->ParentCatalog().IsSystemCatalog())
        return;
    }
    // Compare the caller's settings with the build profile, including when the
    // server allows specific settings to be changed after locking configuration.
    static const std::string names[] = {"arrow_large_buffer_size",
                                        "arrow_lossless_conversion",
                                        "arrow_output_version",
                                        "calendar",
                                        "default_collation",
                                        "old_implicit_casting",
                                        "timezone"};
    std::string current;
    for (const auto& name : names) {
      duckdb::Value value;
      if (!connection.context->TryGetCurrentSetting(name, value) || value.IsNull())
        continue;
      if (!current.empty()) current += ',';
      current += name + '=';
      for (const unsigned char byte : value.ToString()) {
        current += duckdb::Blob::HEX_TABLE[byte >> 4];
        current += duckdb::Blob::HEX_TABLE[byte & 15];
      }
    }
    eligible = current == settings;
  });
  return eligible;
}

}  // namespace

AnalyticsCache::AnalyticsCache(fs::path directory, std::string catalog, std::string epoch,
                               std::string username)
    : directory_(std::move(directory)),
      catalog_(std::move(catalog)),
      epoch_(std::move(epoch)),
      username_(std::move(username)) {}

arrow::Result<std::unique_ptr<AnalyticsCache>> AnalyticsCache::Open(
    const std::string& config_path) {
  try {
    auto config = json::parse(ReadSmallFile(config_path, kMaxMetadataBytes));
    auto state = fs::path(config.at("state_dir").get<std::string>());
    auto catalog = config.at("catalog").get<std::string>();
    auto epoch = config.at("source_epoch").get<std::string>();
    auto user = config.at("user").get<std::string>();
    if (!state.is_absolute() ||
        !std::regex_match(catalog, std::regex("[a-z_][a-z_0-9]*")) || epoch.empty() ||
        user.empty()) {
      return arrow::Status::Invalid("Invalid analytics cache configuration");
    }
    return std::unique_ptr<AnalyticsCache>(
        new AnalyticsCache(fs::weakly_canonical(state), catalog, epoch, user));
  } catch (const std::exception&) {
    return arrow::Status::Invalid("Cannot read analytics cache configuration");
  }
}

bool AnalyticsCache::EligibleSession(const ClientSession& session) const {
  return session.username == username_ && session.catalog_access.empty() &&
         !session.kill_requested.load();
}

std::string AnalyticsCache::TicketProof(const ClientSession& session,
                                        const std::string& sql,
                                        const arrow::Schema& schema) const {
  if (!EligibleSession(session) || sql.size() > kMaxMetadataBytes) return {};
  const auto key_path = directory_ / "identity.key";
  auto& connection = session.connection->Get();
  if (!PrivateFile(connection, key_path) ||
      !PrivateFile(connection, directory_ / "artifacts.duckdb"))
    return {};
  const auto key = ReadSmallFile(key_path, kKeyBytes);
  if (key.size() != kKeyBytes) return {};
  const auto serialized = arrow::ipc::SerializeSchema(schema);
  if (!serialized.ok()) return {};
  return kTicketPrefix +
         SecurityUtilities::HMAC_SHA256(key, "ticket" + kSeparator + session.session_id +
                                                 kSeparator + sql + kSeparator +
                                                 (*serialized)->ToString());
}

std::shared_ptr<arrow::RecordBatchReader> AnalyticsCache::TryRead(
    const ClientSession& session, const std::string& sql, const std::string& ticket_proof,
    const std::shared_ptr<arrow::Schema>& schema,
    duckdb::PreparedStatement& statement) const {
  auto& connection = session.connection->Get();
  if (!ticket_proof.starts_with(kTicketPrefix) || !EligibleSession(session) ||
      statement.GetStatementType() != duckdb::StatementType::SELECT_STATEMENT ||
      !statement.GetStatementProperties().IsReadOnly() ||
      statement.GetStatementProperties().read_databases.size() != 1 ||
      !statement.GetStatementProperties().read_databases.contains(catalog_)) {
    return nullptr;
  }
  try {
    const auto expected = TicketProof(session, sql, *schema);
    if (expected.empty() || expected.size() != ticket_proof.size() ||
        CRYPTO_memcmp(expected.data(), ticket_proof.data(), expected.size()) != 0)
      return nullptr;
    const auto key = ReadSmallFile(directory_ / "identity.key", kKeyBytes);
    if (key.size() != kKeyBytes) return nullptr;
    const auto match = SecurityUtilities::HMAC_SHA256(
        key, "query" + kSeparator + username_ + kSeparator + sql);
    const auto manifest_path = directory_ / "published" / ("match_" + match + ".json");
    if (!PrivateFile(connection, manifest_path)) return nullptr;
    const auto manifest = json::parse(ReadSmallFile(manifest_path, kMaxMetadataBytes));
    if (manifest.at("format") != 2 || manifest.at("epoch") != epoch_ ||
        manifest.at("catalog") != catalog_)
      return nullptr;
    const auto artifact_name = manifest.at("artifact").get<std::string>();
    static const std::regex artifact_pattern("mv_[0-9a-f]{64}_[0-9a-f]{32}\\.arrow");
    if (!std::regex_match(artifact_name, artifact_pattern)) return nullptr;
    const auto artifact = directory_ / "published" / artifact_name;
    const auto settings = manifest.at("settings").get<std::string>();
    if (!PrivateFile(connection, artifact) || !CanUseSourceSnapshot(connection, settings))
      return nullptr;

    // current_snapshot() observes commits from other clients. Match its ID and check
    // the exact metadata row in PostgreSQL without enumerating snapshot history.
    const auto& checked = manifest.at("snapshot");
    if (!checked.is_array() || checked.size() != 3 || !checked[0].is_number_integer())
      return nullptr;
    const auto id = std::to_string(checked[0].get<int64_t>());
    const auto metadata_catalog = manifest.at("metadata_catalog").get<std::string>();
    const auto metadata_schema = manifest.at("metadata_schema").get<std::string>();
    const auto point_lookup =
        "SELECT snapshot_id, snapshot_time, schema_version FROM \"" +
        duckdb::StringUtil::Replace(metadata_schema, "\"", "\"\"") +
        "\".ducklake_snapshot WHERE snapshot_id = " + id;
    // A private connection preserves the Flight session's active result.
    duckdb::Connection probe(*connection.context->db);
    const auto snapshot = probe.Query(
        "SELECT s.snapshot_id, system.main.epoch_us(s.snapshot_time), s.schema_version, "
        "system.main.version(), d.data_path, d.extension_version, db.database_oid "
        "FROM \"" +
        catalog_ + "\".current_snapshot() c JOIN system.main.postgres_query(" +
        duckdb::Value(metadata_catalog).ToSQLString() + ", " +
        duckdb::Value(point_lookup).ToSQLString() +
        ") s ON s.snapshot_id = c.id "
        "CROSS JOIN system.main.ducklake_settings('" +
        catalog_ +
        "') d "
        "JOIN system.main.duckdb_databases() db ON db.database_name = '" +
        catalog_ +
        "' "
        "WHERE NOT system.main.current_setting('enable_external_access') AND "
        "system.main.current_setting('lock_configuration') AND d.catalog_type = "
        "'postgres' "
        "AND db.type = 'ducklake' AND db.readonly");
    if (snapshot->HasError() || snapshot->RowCount() != 1 ||
        !CanUseSourceSnapshot(connection, settings) || session.kill_requested.load())
      return nullptr;
    const auto source = json::array({snapshot->GetValue(0, 0).GetValue<int64_t>(),
                                     snapshot->GetValue(1, 0).GetValue<int64_t>(),
                                     snapshot->GetValue(2, 0).GetValue<int64_t>()});
    if (source != manifest.at("snapshot") ||
        snapshot->GetValue(3, 0).ToString() != manifest.at("engine").get<std::string>() ||
        snapshot->GetValue(4, 0).ToString() !=
            manifest.at("data_path").get<std::string>() ||
        snapshot->GetValue(5, 0).ToString() !=
            manifest.at("extension").get<std::string>() ||
        snapshot->GetValue(6, 0).GetValue<uint64_t>() !=
            statement.GetStatementProperties().read_databases.at(catalog_).catalog_oid)
      return nullptr;

    auto input = arrow::io::ReadableFile::Open(artifact.string());
    if (!input.ok()) return nullptr;
    const auto size = (*input)->GetSize();
    if (!size.ok() || *size > kMaxArtifactBytes) return nullptr;
    const auto file = arrow::ipc::RecordBatchFileReader::Open(*input);
    if (!file.ok() || !(*file)->schema()->Equals(*schema, true)) return nullptr;
    arrow::RecordBatchVector batches;
    for (int i = 0; i < (*file)->num_record_batches(); ++i) {
      auto batch = (*file)->ReadRecordBatch(i);
      if (!batch.ok() || !(*batch)->ValidateFull().ok()) return nullptr;
      batches.push_back(std::move(*batch));
    }
    auto reader = arrow::RecordBatchReader::Make(std::move(batches), schema);
    return reader.ok() ? std::move(*reader) : nullptr;
  } catch (const std::exception&) {
    return nullptr;  // An unavailable or invalid artifact is an ordinary source read.
  }
}

}  // namespace gizmosql::ddb

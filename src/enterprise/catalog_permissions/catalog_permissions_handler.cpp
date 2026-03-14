// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "catalog_permissions_handler.h"

#include <algorithm>
#include <sstream>
#include <unordered_map>

#include "gizmosql_logging.h"
#include "session_context.h"
#include "enterprise/enterprise_features.h"
#include "instrumentation/instrumentation_manager.h"
#include "instrumentation/instrumentation_records.h"

namespace gizmosql::enterprise {

namespace {

void AddCatalogIfMissing(std::vector<std::string>& catalogs, const std::string& catalog_name) {
  if (std::find(catalogs.begin(), catalogs.end(), catalog_name) == catalogs.end()) {
    catalogs.push_back(catalog_name);
  }
}

void RemoveCatalogIfPresent(std::vector<std::string>& catalogs, const std::string& catalog_name) {
  catalogs.erase(std::remove(catalogs.begin(), catalogs.end(), catalog_name), catalogs.end());
}

std::string GetReadAccessDeniedMessage(
    const ClientSession& client_session,
    const std::string& catalog_name,
    const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager) {
  if (instrumentation_manager && catalog_name == instrumentation_manager->GetCatalog() &&
      client_session.role != "admin") {
    return "Access denied: Only administrators can read the instrumentation catalog '" +
           catalog_name + "'.";
  }

  return "Access denied: You do not have read access to catalog '" + catalog_name + "'.";
}

}  // namespace

bool MetadataCatalogFilter::IsRestricted() const {
  return mode == MetadataCatalogFilterMode::kAllowOnly ||
         (mode == MetadataCatalogFilterMode::kAllowAllExcept && !catalogs.empty());
}

bool MetadataCatalogFilter::Allows(const std::string& catalog_name) const {
  const bool contains =
      std::find(catalogs.begin(), catalogs.end(), catalog_name) != catalogs.end();

  switch (mode) {
    case MetadataCatalogFilterMode::kAllowAll:
      return true;
    case MetadataCatalogFilterMode::kAllowOnly:
      return contains;
    case MetadataCatalogFilterMode::kAllowAllExcept:
      return !contains;
  }

  return true;
}

CatalogAccessLevel GetCatalogAccess(
    const std::string& catalog_name,
    const std::string& role,
    const std::vector<CatalogAccessRule>& catalog_access,
    const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager) {
  // The instrumentation catalog is special: system-managed, read-only for admins.
  // This protection ALWAYS applies, regardless of licensing or token rules.
  // The catalog name is configurable (e.g., DuckLake catalogs), so we check dynamically.
  if (instrumentation_manager && catalog_name == instrumentation_manager->GetCatalog()) {
    return (role == "admin") ? CatalogAccessLevel::kRead : CatalogAccessLevel::kNone;
  }

  // Runtime check: if catalog permissions feature is not licensed, grant full access
  // to all other catalogs (backward compatible with Core edition behavior)
  if (!EnterpriseFeatures::Instance().IsCatalogPermissionsAvailable()) {
    return CatalogAccessLevel::kWrite;
  }

  // If no catalog_access rules defined, grant full access (backward compatible)
  if (catalog_access.empty()) {
    return CatalogAccessLevel::kWrite;
  }

  // Check rules in order - first match wins
  for (const auto& rule : catalog_access) {
    if (rule.catalog == catalog_name || rule.catalog == "*") {
      return rule.access;
    }
  }

  // No matching rule - deny access
  return CatalogAccessLevel::kNone;
}

bool HasReadAccess(const ClientSession& client_session, const std::string& catalog_name,
                   const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager) {
  auto access = GetCatalogAccess(catalog_name, client_session.role, client_session.catalog_access,
                                 instrumentation_manager);
  return access >= CatalogAccessLevel::kRead;
}

bool HasWriteAccess(const ClientSession& client_session, const std::string& catalog_name,
                    const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager) {
  auto access = GetCatalogAccess(catalog_name, client_session.role, client_session.catalog_access,
                                 instrumentation_manager);
  return access >= CatalogAccessLevel::kWrite;
}

MetadataCatalogFilter GetMetadataCatalogFilter(
    const ClientSession& client_session,
    const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager) {
  MetadataCatalogFilter filter;

  const bool catalog_permissions_enabled =
      EnterpriseFeatures::Instance().IsCatalogPermissionsAvailable();

  if (!catalog_permissions_enabled || client_session.catalog_access.empty()) {
    filter.mode = MetadataCatalogFilterMode::kAllowAll;
  } else {
    std::unordered_map<std::string, CatalogAccessLevel> explicit_rules;
    bool wildcard_seen = false;
    CatalogAccessLevel wildcard_access = CatalogAccessLevel::kNone;

    for (const auto& rule : client_session.catalog_access) {
      if (rule.catalog == "*") {
        wildcard_seen = true;
        wildcard_access = rule.access;
        break;
      }

      explicit_rules.emplace(rule.catalog, rule.access);
    }

    if (!wildcard_seen || wildcard_access < CatalogAccessLevel::kRead) {
      filter.mode = MetadataCatalogFilterMode::kAllowOnly;
      for (const auto& [catalog_name, access] : explicit_rules) {
        if (access >= CatalogAccessLevel::kRead) {
          filter.catalogs.push_back(catalog_name);
        }
      }
    } else {
      filter.mode = MetadataCatalogFilterMode::kAllowAllExcept;
      for (const auto& [catalog_name, access] : explicit_rules) {
        if (access < CatalogAccessLevel::kRead) {
          filter.catalogs.push_back(catalog_name);
        }
      }
      if (filter.catalogs.empty()) {
        filter.mode = MetadataCatalogFilterMode::kAllowAll;
      }
    }
  }

  if (instrumentation_manager) {
    const auto& instrumentation_catalog = instrumentation_manager->GetCatalog();
    if (client_session.role == "admin") {
      if (filter.mode == MetadataCatalogFilterMode::kAllowOnly) {
        AddCatalogIfMissing(filter.catalogs, instrumentation_catalog);
      } else if (filter.mode == MetadataCatalogFilterMode::kAllowAllExcept) {
        RemoveCatalogIfPresent(filter.catalogs, instrumentation_catalog);
        if (filter.catalogs.empty()) {
          filter.mode = MetadataCatalogFilterMode::kAllowAll;
        }
      }
    } else {
      if (filter.mode == MetadataCatalogFilterMode::kAllowAll) {
        filter.mode = MetadataCatalogFilterMode::kAllowAllExcept;
        filter.catalogs.push_back(instrumentation_catalog);
      } else if (filter.mode == MetadataCatalogFilterMode::kAllowOnly) {
        RemoveCatalogIfPresent(filter.catalogs, instrumentation_catalog);
      } else {
        AddCatalogIfMissing(filter.catalogs, instrumentation_catalog);
      }
    }
  }

  std::sort(filter.catalogs.begin(), filter.catalogs.end());
  return filter;
}

std::string BuildMetadataCatalogFilterSql(
    const MetadataCatalogFilter& filter,
    const std::string& column_name,
    duckdb::vector<duckdb::Value>& bind_parameters) {
  if (!filter.IsRestricted()) {
    return "";
  }

  if (filter.mode == MetadataCatalogFilterMode::kAllowOnly && filter.catalogs.empty()) {
    return " AND 1 = 0";
  }

  if (filter.catalogs.empty()) {
    return "";
  }

  std::stringstream predicate;
  predicate << " AND " << column_name << " "
            << (filter.mode == MetadataCatalogFilterMode::kAllowOnly ? "IN (" : "NOT IN (");

  for (size_t i = 0; i < filter.catalogs.size(); ++i) {
    if (i > 0) {
      predicate << ", ";
    }
    predicate << "?";
    bind_parameters.emplace_back(filter.catalogs[i]);
  }

  predicate << ")";
  return predicate.str();
}

arrow::Status EnsureCatalogReadAccess(
    const ClientSession& client_session,
    const std::string& catalog_name,
    const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager) {
  if (HasReadAccess(client_session, catalog_name, instrumentation_manager)) {
    return arrow::Status::OK();
  }

  return arrow::Status::Invalid(
      GetReadAccessDeniedMessage(client_session, catalog_name, instrumentation_manager));
}

arrow::Status CheckCatalogWriteAccess(
    const std::shared_ptr<ClientSession>& client_session,
    const std::unordered_map<std::string, duckdb::StatementProperties::CatalogIdentity>& modified_databases,
    std::shared_ptr<gizmosql::ddb::InstrumentationManager> instrumentation_manager,
    const std::string& statement_id,
    const std::string& logged_sql,
    const std::string& flight_method,
    bool is_internal) {

  for (const auto& [catalog_name, catalog_identity] : modified_databases) {
    // Block writes to the instrumentation catalog (regardless of other rules)
    // This protects both file-based (_gizmosql_instr) and external catalogs (e.g., DuckLake)
    if (instrumentation_manager && catalog_name == instrumentation_manager->GetCatalog()) {
      GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                             "Access denied: instrumentation catalog is read-only",
                             {"kind", "sql"}, {"status", "rejected"},
                             {"catalog", catalog_name}, {"statement_id", statement_id},
                             {"sql", logged_sql});

      std::string error_msg =
          "Access denied: The instrumentation catalog '" + catalog_name + "' is read-only.";

      // Record the rejected modification attempt
      gizmosql::ddb::StatementInstrumentation(
          instrumentation_manager, statement_id, client_session->session_id,
          logged_sql, flight_method, is_internal, error_msg);

      return arrow::Status::Invalid(error_msg);
    }

    if (!HasWriteAccess(*client_session, catalog_name, instrumentation_manager)) {
      GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                             "Access denied: user lacks write access to catalog",
                             {"kind", "sql"}, {"status", "rejected"},
                             {"catalog", catalog_name}, {"statement_id", statement_id},
                             {"sql", logged_sql});

      std::string error_msg =
          "Access denied: You do not have write access to catalog '" + catalog_name + "'.";

      // Record the rejected modification attempt
      if (instrumentation_manager) {
        gizmosql::ddb::StatementInstrumentation(
            instrumentation_manager, statement_id, client_session->session_id,
            logged_sql, flight_method, is_internal, error_msg);
      }

      return arrow::Status::Invalid(error_msg);
    }
  }

  return arrow::Status::OK();
}

arrow::Status CheckCatalogReadAccess(
    const std::shared_ptr<ClientSession>& client_session,
    const std::unordered_map<std::string, duckdb::StatementProperties::CatalogIdentity>& read_databases,
    std::shared_ptr<gizmosql::ddb::InstrumentationManager> instrumentation_manager,
    const std::string& statement_id,
    const std::string& logged_sql,
    const std::string& flight_method,
    bool is_internal) {

  for (const auto& [catalog_name, catalog_identity] : read_databases) {
    // For the instrumentation catalog, only admins can read
    // This protects both file-based (_gizmosql_instr) and external catalogs (e.g., DuckLake)
    if (instrumentation_manager && catalog_name == instrumentation_manager->GetCatalog()) {
      if (client_session->role != "admin") {
        GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                               "Access denied: only admins can read instrumentation catalog",
                               {"kind", "sql"}, {"status", "rejected"},
                               {"catalog", catalog_name}, {"statement_id", statement_id},
                               {"sql", logged_sql});

        std::string error_msg =
            "Access denied: Only administrators can read the instrumentation catalog '" + catalog_name + "'.";

        // Record the rejected read attempt
        gizmosql::ddb::StatementInstrumentation(
            instrumentation_manager, statement_id, client_session->session_id,
            logged_sql, flight_method, is_internal, error_msg);

        return arrow::Status::Invalid(error_msg);
      }
      // Admin can read instrumentation catalog, skip other checks for this catalog
      continue;
    }

    if (!HasReadAccess(*client_session, catalog_name, instrumentation_manager)) {
      GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                             "Access denied: user lacks read access to catalog",
                             {"kind", "sql"}, {"status", "rejected"},
                             {"catalog", catalog_name}, {"statement_id", statement_id},
                             {"sql", logged_sql});

      std::string error_msg =
          "Access denied: You do not have read access to catalog '" + catalog_name + "'.";

      // Record the rejected read attempt
      if (instrumentation_manager) {
        gizmosql::ddb::StatementInstrumentation(
            instrumentation_manager, statement_id, client_session->session_id,
            logged_sql, flight_method, is_internal, error_msg);
      }

      return arrow::Status::Invalid(error_msg);
    }
  }

  return arrow::Status::OK();
}

}  // namespace gizmosql::enterprise

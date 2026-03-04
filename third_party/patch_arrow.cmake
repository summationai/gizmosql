# Cross-platform patch for Arrow 23's ThirdpartyToolchain.cmake
# Removes problematic set_target_properties on ALIAS target (c-ares::cares)
# and LIBRESOLV_LIBRARY references that break the build.
#
# This replaces the POSIX sed-based patch command for Windows compatibility.

set(TOOLCHAIN_FILE "${ARROW_SOURCE_DIR}/cpp/cmake_modules/ThirdpartyToolchain.cmake")

file(READ "${TOOLCHAIN_FILE}" CONTENT)

# Remove lines containing set_target_properties on c-ares::cares ALIAS target
string(REGEX REPLACE "[^\n]*set_target_properties[^\n]*c-ares::cares[^\n]*PROPERTIES[^\n]*\n" "" CONTENT "${CONTENT}")

# Remove lines referencing LIBRESOLV_LIBRARY
string(REGEX REPLACE "[^\n]*LIBRESOLV_LIBRARY[^\n]*\n" "" CONTENT "${CONTENT}")

file(WRITE "${TOOLCHAIN_FILE}" "${CONTENT}")

# Fix: Arrow's SetupCxxFlags.cmake unconditionally adds -D__SSE2__ -D__SSE4_1__
# -D__SSE4_2__ in the MSVC block, even when ARROW_SIMD_LEVEL=NONE. These defines
# cause bundled Abseil to include x86intrin.h, which is a GCC/Clang header that
# MSVC doesn't have. Wrap the add_definitions in a SIMD level check.
set(CXX_FLAGS_FILE "${ARROW_SOURCE_DIR}/cpp/cmake_modules/SetupCxxFlags.cmake")

file(READ "${CXX_FLAGS_FILE}" CXX_FLAGS_CONTENT)

string(REPLACE
  "add_definitions(-D__SSE2__ -D__SSE4_1__ -D__SSE4_2__)"
  "if(NOT ARROW_SIMD_LEVEL STREQUAL \"NONE\")\n      add_definitions(-D__SSE2__ -D__SSE4_1__ -D__SSE4_2__)\n    endif()"
  CXX_FLAGS_CONTENT
  "${CXX_FLAGS_CONTENT}"
)

file(WRITE "${CXX_FLAGS_FILE}" "${CXX_FLAGS_CONTENT}")

# GizmoSQL compatibility patch:
# Support generic Flight DoPut PATH descriptors by mapping them to
# Flight SQL StatementIngest semantics in Arrow's FlightSqlServerBase::DoPut.
set(FLIGHT_SQL_SERVER_FILE "${ARROW_SOURCE_DIR}/cpp/src/arrow/flight/sql/server.cc")
file(READ "${FLIGHT_SQL_SERVER_FILE}" FLIGHT_SQL_SERVER_CONTENT)
set(ORIGINAL_FLIGHT_SQL_SERVER_CONTENT "${FLIGHT_SQL_SERVER_CONTENT}")

set(DOPUT_PATCH_OLD [[Status FlightSqlServerBase::DoPut(const ServerCallContext& context,
                                  std::unique_ptr<FlightMessageReader> reader,
                                  std::unique_ptr<FlightMetadataWriter> writer) {
  const FlightDescriptor& request = reader->descriptor();

  google::protobuf::Any any;
  if (!any.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))) {
    return Status::Invalid("Unable to parse command");
  }
]])

set(DOPUT_PATCH_NEW [[Status FlightSqlServerBase::DoPut(const ServerCallContext& context,
                                  std::unique_ptr<FlightMessageReader> reader,
                                  std::unique_ptr<FlightMetadataWriter> writer) {
  const FlightDescriptor& request = reader->descriptor();

  // GizmoSQL patch: support DoPut PATH descriptors by translating them into
  // StatementIngest requests with append/create defaults.
  if (request.type == FlightDescriptor::PATH) {
    if (request.path.empty()) {
      return Status::Invalid("Unable to parse command");
    }

    std::vector<std::string> path_parts = request.path;
    if (path_parts.size() == 1) {
      std::vector<std::string> split_parts;
      std::string current_part;
      for (const char c : path_parts[0]) {
        if (c == '.') {
          if (current_part.empty()) {
            return Status::Invalid("Unable to parse command");
          }
          split_parts.push_back(current_part);
          current_part.clear();
          continue;
        }
        current_part.push_back(c);
      }
      if (current_part.empty()) {
        return Status::Invalid("Unable to parse command");
      }
      split_parts.push_back(current_part);
      path_parts = std::move(split_parts);
    }

    if (path_parts.empty() || path_parts.size() > 3) {
      return Status::Invalid("Unable to parse command");
    }

    StatementIngest internal_command;
    internal_command.table_definition_options.if_not_exist =
        TableDefinitionOptionsTableNotExistOption::kCreate;
    internal_command.table_definition_options.if_exists =
        TableDefinitionOptionsTableExistsOption::kAppend;
    internal_command.table = path_parts[path_parts.size() - 1];
    if (path_parts.size() >= 2) {
      internal_command.schema = path_parts[path_parts.size() - 2];
    }
    if (path_parts.size() == 3) {
      internal_command.catalog = path_parts[0];
    }

    ARROW_ASSIGN_OR_RAISE(
        auto record_count,
        DoPutCommandStatementIngest(context, internal_command, reader.get()));

    pb::sql::DoPutUpdateResult result;
    result.set_record_count(record_count);

    const auto buffer = Buffer::FromString(result.SerializeAsString());
    ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));
    return Status::OK();
  }

  google::protobuf::Any any;
  if (!any.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))) {
    return Status::Invalid("Unable to parse command");
  }
]])

string(REPLACE "${DOPUT_PATCH_OLD}" "${DOPUT_PATCH_NEW}"
       FLIGHT_SQL_SERVER_CONTENT "${FLIGHT_SQL_SERVER_CONTENT}")

if(FLIGHT_SQL_SERVER_CONTENT STREQUAL ORIGINAL_FLIGHT_SQL_SERVER_CONTENT)
  if(FLIGHT_SQL_SERVER_CONTENT MATCHES "GizmoSQL patch: support DoPut PATH descriptors")
    message(STATUS "Arrow DoPut PATH compatibility patch already present")
  else()
    message(FATAL_ERROR "Failed to apply Arrow DoPut PATH compatibility patch")
  endif()
endif()

file(WRITE "${FLIGHT_SQL_SERVER_FILE}" "${FLIGHT_SQL_SERVER_CONTENT}")

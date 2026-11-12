// Example demonstrating how to use the Snowflake Streaming Ingest SDK in C++
// with the high-performance architecture and default pipe.
//
// The default pipe is automatically created by Snowflake when you first
// open a channel. No CREATE PIPE DDL is required. The default pipe name
// follows the convention: <TABLE_NAME>-STREAMING

#include <snowflake/ingest/streaming/client.h>

#include <chrono>
#include <cstdlib>
#include <ctime>
#include <future>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <string>

namespace sn = snowflake::ingest::streaming;

namespace {

constexpr int kMaxRows = 10;

// Replace these with your Snowflake object names
constexpr const char* kDatabase = "MY_DATABASE";
constexpr const char* kSchema = "MY_SCHEMA";
constexpr const char* kTable = "MY_TABLE";

// Default pipe: Snowflake auto-creates this on first channel open
const std::string kPipe = std::string(kTable) + "-STREAMING";

std::string RandomSuffix() {
  std::random_device rd;
  return std::to_string(std::mt19937_64(rd())());
}

// Current local time as a TIMESTAMP_NTZ string, for example 2026-01-01 12:00:00.
std::string Now() {
  std::time_t now = std::time(nullptr);
  std::tm local{};
  localtime_r(&now, &local);
  std::ostringstream out;
  out << std::put_time(&local, "%Y-%m-%d %H:%M:%S");
  return out.str();
}

// Prints the error if the call failed. Returns whether it succeeded.
template <typename T>
bool Ok(const sn::Result<T>& result, const char* what) {
  if (!result.has_value()) std::cerr << what << " failed: " << result.error().message() << "\n";
  return result.has_value();
}

}  // namespace

int main() {
  // Set SS_LOG_LEVEL=info in the environment to increase logging details.
  // Process-wide settings must be in place before the client is created.
  setenv("SS_LOG_LEVEL", "warn", /*overwrite=*/0);

  sn::ClientOptions options;
  options.client_name = "MY_CLIENT_" + RandomSuffix();
  options.db_name = kDatabase;
  options.schema_name = kSchema;
  options.pipe_name = kPipe;
  options.profile_json = "profile.json";

  sn::Result<std::unique_ptr<sn::StreamingIngestClient>> client_result =
      sn::StreamingIngestClient::Create(options);
  if (!Ok(client_result, "Create client")) return 1;
  sn::StreamingIngestClient& client = **client_result;
  std::cout << "Client created successfully\n";

  // A fresh name isolates this demo run. Restart recovery needs stable ownership
  // and the committed offset returned by OpenChannel, not a new random name.
  sn::Result<std::pair<std::unique_ptr<sn::StreamingIngestChannel>, sn::ChannelStatus>>
      channel_result = client.OpenChannel("MY_CHANNEL_" + RandomSuffix());
  if (!Ok(channel_result, "OpenChannel")) return 1;
  sn::StreamingIngestChannel& channel = *channel_result->first;
  std::cout << "Channel opened: " << channel.channel_name() << "\n";

  // Each row is a JSON object string; keys must match the target table columns.
  // The default pipe uses MATCH_BY_COLUMN_NAME to map fields.
  std::cout << "Ingesting " << kMaxRows << " rows...\n";
  for (int i = 1; i <= kMaxRows; ++i) {
    // Offsets identify source positions; they are not automatic deduplication keys.
    const std::string row_id = std::to_string(i);
    const std::string row = R"({"c1": )" + row_id + R"(, "c2": ")" + row_id +
                            R"(", "ts": ")" + Now() + R"("})";
    if (!Ok(channel.AppendRow(row, row_id), "AppendRow")) return 1;
  }

  std::cout << "All rows submitted. Waiting for commit...\n";

  // The predicate receives the latest committed offset token and returns true
  // when satisfied. Timeout does not cancel ingestion or prove failure.
  // Do not blindly replay.
  sn::Result<std::future<sn::Result<void>>> commit_future = channel.WaitForCommit(
      [](const std::optional<std::string>& token) {
        return token.has_value() && std::stoll(*token) >= kMaxRows;
      },
      std::chrono::seconds(30));
  if (!Ok(commit_future, "WaitForCommit") || !Ok(commit_future->get(), "WaitForCommit")) return 1;

  // Now that data has landed, check the channel status
  sn::Result<sn::ChannelStatus> status_result = channel.GetChannelStatus();
  if (!Ok(status_result, "GetChannelStatus")) return 1;
  const sn::ChannelStatus& status = *status_result;
  std::cout << "All data committed. Channel status:\n";
  std::cout << "  Committed offset:   " << status.latest_committed_offset_token.value_or("<none>") << "\n";
  std::cout << "  Rows inserted:      " << status.rows_inserted_count << "\n";
  std::cout << "  Rows errored:       " << status.rows_error_count << "\n";
  if (status.snowflake_avg_processing_latency.has_value()) {
    std::cout << "  Avg server latency: " << status.snowflake_avg_processing_latency->count() << " ms\n";
  }
  if (status.rows_error_count > 0) {
    std::cout << "  Last error:         " << status.last_error_message.value_or("<none>") << "\n";
  }

  // On the early returns above, the channel and client destructors close them.
  // Close explicitly here to wait for the flush and see any error.
  if (!Ok(channel.Close(), "Close channel") || !Ok(client.Close(), "Close client")) return 1;
  std::cout << "Data ingestion completed\n";
  return 0;
}

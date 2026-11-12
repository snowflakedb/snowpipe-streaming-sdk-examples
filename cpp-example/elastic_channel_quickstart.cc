// Send ten rows through an Elastic channel and wait for durable acknowledgements.

#include <snowflake/ingest/streaming/client.h>
#include <snowflake/ingest/streaming/elastic_channel.h>

#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace sn = snowflake::ingest::streaming;

namespace {

// Replace these with your existing target table. Authentication lives in profile.json.
constexpr const char* kDatabase = "MY_DATABASE";
constexpr const char* kSchema = "MY_SCHEMA";
constexpr const char* kTable = "MY_TABLE";

std::string RandomSuffix() {
  std::random_device rd;
  return std::to_string(std::mt19937_64(rd())());
}

// Prints the error if the call failed. Returns whether it succeeded.
template <typename T>
bool Ok(const sn::Result<T>& result, const char* what) {
  if (!result.has_value()) std::cerr << what << " failed: " << result.error().message() << "\n";
  return result.has_value();
}

}  // namespace

int main() {
  // The table client uses the default streaming pipe; no CREATE PIPE is needed.
  sn::TableClientOptions options;
  options.client_name = "quickstart-" + RandomSuffix();
  options.db_name = kDatabase;
  options.schema_name = kSchema;
  options.table_name = kTable;
  options.profile_json = "profile.json";

  sn::Result<std::unique_ptr<sn::StreamingIngestClient>> client_result =
      sn::StreamingIngestClient::CreateFromTable(options);
  if (!Ok(client_result, "Create client")) return 1;
  sn::StreamingIngestClient& client = **client_result;

  // Elastic channels are managed by Snowflake, so there is no channel name to choose.
  sn::Result<std::shared_ptr<sn::StreamingIngestElasticChannel>> channel_result =
      client.GetElasticChannel();
  if (!Ok(channel_result, "GetElasticChannel")) return 1;
  std::shared_ptr<sn::StreamingIngestElasticChannel> channel = *channel_result;

  std::vector<std::future<sn::Result<void>>> pending;
  for (int event_id = 1; event_id <= 10; ++event_id) {
    // Replace sample values with source data; keys match the target columns.
    const std::string id = std::to_string(event_id);
    const std::string row = R"({"C1": )" + id + R"(, "C2": ")" + id + R"("})";
    // Submit before waiting so the SDK can batch. std::nullopt disables callback token reporting.
    sn::Result<std::future<sn::Result<void>>> ack_future =
        channel->AppendRowWithWait(row, std::nullopt);
    if (!Ok(ack_future, "AppendRowWithWait")) return 1;
    pending.push_back(std::move(*ack_future));
  }

  // A slow acknowledgement is not a failed append; do not resubmit on a local timeout.
  bool complete = true;
  for (std::future<sn::Result<void>>& ack_future : pending) {
    complete = Ok(ack_future.get(), "Append") && complete;
  }
  if (complete) std::cout << "Durably acknowledged 10 rows. Check table contents separately.\n";

  // Always release resources. On failure, retain unconfirmed source data for recovery.
  // On the early returns above, the client destructor closes it.
  complete = Ok(client.Close(/*wait_for_flush=*/complete, std::chrono::seconds(30)), "Close client") && complete;
  return complete ? 0 : 1;
}

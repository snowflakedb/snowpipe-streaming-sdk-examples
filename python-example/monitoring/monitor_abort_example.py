import time
import os
from typing import Optional
from faker import Faker

try:
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# Must be set before importing StreamingIngestClient
os.environ["SS_LOG_LEVEL"] = "warn"
os.environ["SS_ENABLE_METRICS"] = "true"
from snowflake.ingest.streaming import StreamingIngestClient

# Configuration
DATABASE = "MY_DATABASE"
SCHEMA = "MY_SCHEMA"
PIPE = "MY_TABLE-STREAMING"
PROFILE_JSON_PATH = "profile.json"
CHANNEL_NAME = "monitor_abort_channel"
ENABLE_PLOTTING = False  # Set to True for live matplotlib lag chart

TOTAL_ROWS = 500
SEND_INTERVAL_MS = 100
POLL_MS = 500
TEST_ERROR_OFFSET = -1  # -1 to disable test error injection


fake = Faker()

def make_row(i: int) -> dict:
    return {
        "user_id": i,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "address": fake.address().replace("\n", ", "),
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
        "registration_date": fake.date_time_this_year().isoformat(),
        "city": fake.city(),
        "state": fake.state(),
        "country": fake.country(),
    }

class OffsetTracker:
    def __init__(self) -> None:
        self.sent_highest_offset: int = 0

    def update_sent(self, offset: int) -> None:
        if offset > self.sent_highest_offset:
            self.sent_highest_offset = offset

    def compute_lag(self, committed_token: Optional[str]) -> Optional[int]:
        try:
            committed = None if committed_token is None else int(committed_token)
        except (TypeError, ValueError):
            return None
        if committed is None:
            return None
        return self.sent_highest_offset - committed

class LagPlotter:
    def __init__(self, enabled: bool):
        self.enabled = enabled and MATPLOTLIB_AVAILABLE
        self.fig = None
        self.ax = None
        self.line = None
        self.x_data = []
        self.y_data = []

    def setup(self) -> None:
        if not self.enabled:
            return
        plt.ion()
        self.fig, self.ax = plt.subplots()
        self.ax.set_title("Offset Lag (sent - committed)")
        self.ax.set_xlabel("Sent offset")
        self.ax.set_ylabel("Lag")
        (self.line,) = self.ax.plot([], [], label="lag")
        self.ax.legend(loc="upper right")

    def add_point(self, x_value: int, y_value: int) -> None:
        if not self.enabled or self.line is None:
            return
        self.x_data.append(x_value)
        self.y_data.append(y_value)
        self.line.set_data(self.x_data, self.y_data)
        self.ax.relim()
        self.ax.autoscale_view()
        plt.pause(0.001)

    def finalize(self) -> None:
        if not self.enabled:
            return
        plt.ioff()
        plt.show()

def monitor_channel_status(status, tracker: OffsetTracker, plotter: LagPlotter, error_count_prev: Optional[int]) -> tuple[Optional[int], bool]:
    """Log channel status, update plot, and detect error-count increase.
    Returns (new_error_count_prev, abort_requested).
    """
    committed_token = status.latest_committed_offset_token
    lag = tracker.compute_lag(committed_token)

    print()
    print("[monitor] committed=", committed_token)
    print("[monitor] rows_inserted=", status.rows_inserted_count)
    print("[monitor] rows_error_count=", status.rows_error_count)
    print("[monitor] server_avg_processing_latency=", status.server_avg_processing_latency)
    print("[monitor] last_error_offset_token_upper_bound=", status.last_error_offset_token_upper_bound)
    print("[monitor] last_error_message=", status.last_error_message)
    if lag is not None:
        print("[monitor] offset_lag=", lag)
    else:
        print("[monitor] offset_lag=", "N/A")
    print()

    if lag is not None:
        plotter.add_point(tracker.sent_highest_offset, lag)

    current_error_count = status.rows_error_count
    if error_count_prev is None:
        return current_error_count, False
    if current_error_count > error_count_prev:
        print("[monitor] Error count increased (", error_count_prev, "->", current_error_count, ") - halting production.")
        print("Monitor requested to halt production - current offset not yet sent=", tracker.sent_highest_offset + 1)
        return current_error_count, True
    return current_error_count, False

def main(total_rows: int = TOTAL_ROWS, send_interval_ms: int = SEND_INTERVAL_MS, poll_ms: int = POLL_MS) -> None:
    client = StreamingIngestClient(
        "MY_CLIENT",
        DATABASE,
        SCHEMA,
        PIPE,
        profile_json=PROFILE_JSON_PATH,
        properties=None,
    )

    channel, status = client.open_channel(CHANNEL_NAME)
    print("Channel:", status.channel_name, "status:", status.status_code)

    # If channel already has committed state, drop and reopen for a fresh run
    initial_status = channel.get_channel_status()
    if initial_status.latest_committed_offset_token is not None:
        print("Existing channel state detected; dropping and reopening for a fresh run...")
        channel.close(drop=True)
        time.sleep(0.5)
        channel, status = client.open_channel(CHANNEL_NAME)
        print("Reopened channel:", status.channel_name, "status:", status.status_code)
        time.sleep(0.5)

    plotter = LagPlotter(ENABLE_PLOTTING)
    plotter.setup()

    tracker = OffsetTracker()
    last_poll_time = 0.0
    error_count_prev: Optional[int] = None

    # Producer loop
    for i in range(1, total_rows + 1):
        row = make_row(i)
        # If TEST_ERROR_OFFSET is set, inject a deliberate schema error for exactly one row
        if i == TEST_ERROR_OFFSET:
            row["user_id"] = "BAD_ID"           # should be INTEGER
            row["date_of_birth"] = "INVALID_DATE"  # should be DATE
            row["registration_date"] = "INVALID_TS"  # should be TIMESTAMP_NTZ
            print("[producer] Injecting schema error at offset=", i)
        channel.append_row(row, offset_token=str(i))
        tracker.update_sent(i)
        if i % 10 == 0:
            print(f"[producer] sent offset={i}")

        # Monitor loop
        now = time.time()
        if now - last_poll_time >= (poll_ms / 1000):
            status = channel.get_channel_status()
            error_count_prev, abort = monitor_channel_status(status, tracker, plotter, error_count_prev)
            if abort:
                break
            last_poll_time = now

        time.sleep(send_interval_ms / 1000)

    # Wait for commits to catch up
    print(f"Waiting for commits to catch up to last sent... {tracker.sent_highest_offset}")

    def token_reached(latest_token):
        try:
            return latest_token is not None and int(latest_token) >= tracker.sent_highest_offset
        except (TypeError, ValueError):
            return False

    channel.wait_for_commit(token_reached, timeout_seconds=15)

    print("Commits caught up to last sent.")
    print("Final committed:", channel.get_latest_committed_offset_token())

    channel.close()
    # Drop channel for demo cleanup. In production, you typically reopen
    # existing channels to resume from the last committed offset.
    client.drop_channel(CHANNEL_NAME)

    client.close()
    plotter.finalize()
    print("Monitor-abort demo completed.")


if __name__ == "__main__":
    main()

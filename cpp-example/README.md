# C++ Streaming Quickstarts

Two self-contained examples send ten rows using C++ SDK 1.9.4: an Elastic channel
quickstart and a named channel quickstart. The SDK requires C++ version 17 or later.
The examples also require CMake 3.21+, and glibc 2.26+ on Linux.

## Getting SDK

Download the C++ Snowpipe Streaming SDK from the
[Snowflake Client Repository](https://sfc-repo.snowflakecomputing.com/snowpipe-streaming-cpp/index.html).

Each package in the repository has a matching `.sha256` checksum file and `.asc`
signature file. The following commands download all three files into the current
directory, then validate and unpack the package. Set `SDK_VERSION` to the version
you want, `PLATFORM` to your package's platform, and `PLATFORM_DIR` to the matching
repository directory:

| `PLATFORM` | `PLATFORM_DIR` |
|---|---|
| `Linux-x86_64` | `linux_x86_64` |
| `Linux-aarch64` | `linux_aarch64` |
| `Darwin-arm64` | `macos_arm64` |

Set `GPG_KEY_ID` to the ID of the Snowflake public key that signed the package.
Packages are signed with the Snowflake public key `3C98F63C9292CE02`.

```bash
SDK_VERSION=1.9.4
PLATFORM=Linux-x86_64
PLATFORM_DIR=linux_x86_64
GPG_KEY_ID=3C98F63C9292CE02
PACKAGE=snowflake-streaming-ingest-cpp-${SDK_VERSION}-${PLATFORM}.tar.gz
BASE_URL=https://sfc-repo.snowflakecomputing.com/snowpipe-streaming-cpp/${SDK_VERSION}/${PLATFORM_DIR}

# Download the package, checksum, and signature into the current directory.
curl -fLO "${BASE_URL}/${PACKAGE}"
curl -fLO "${BASE_URL}/${PACKAGE}.sha256"
curl -fLO "${BASE_URL}/${PACKAGE}.asc"

# Verify the checksum. On macOS, run: shasum -a 256 -c "${PACKAGE}.sha256"
sha256sum -c "${PACKAGE}.sha256"

# Import the Snowflake public key, then verify the signature.
gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys "${GPG_KEY_ID}"
gpg --verify "${PACKAGE}.asc" "${PACKAGE}"

tar -xzf "${PACKAGE}"
SDK_DIR="$PWD/snowflake-streaming-ingest-cpp-${SDK_VERSION}-${PLATFORM}"
```

Do not use the package if either check fails.

Keep the unpacked package directory intact when you build. The package's CMake
configuration file locates the headers and libraries by their paths inside the
package, so move or rename the whole directory rather than copying individual
files out of it.

## Setup

Create `profile.json` using `profile.json.example`. Configure your account, user,
role, and private key for key-pair authentication. Register the matching public
key on the Snowflake user. Keep credentials and local profiles out of version control.
See [key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth).

Create a table in an existing database/schema using an authorized role:

```sql
CREATE TABLE MY_DATABASE.MY_SCHEMA.MY_TABLE (
    C1 NUMBER,
    C2 VARCHAR,
    TS TIMESTAMP_NTZ
);
```

Set `kDatabase`, `kSchema`, and `kTable` at the top of the example you want to run.
Both examples use the default streaming pipe; no CREATE PIPE statement is needed.
Build with `CMAKE_PREFIX_PATH` set to the absolute path of the unpacked package
directory (`SDK_DIR` from the step above, not the directory where you ran `tar`),
then run from this language directory so `profile.json` resolves correctly:

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH="$SDK_DIR"
cmake --build build
./build/elastic_channel_quickstart
./build/named_channel_quickstart
```

## Choose a Quickstart

Each row is passed as a JSON-shaped string whose keys match the target columns.

- **Elastic:** generates rows, submits them before waiting, observes durable
  acknowledgements, and closes the client. The SDK batches and retries transport
  operations. No application channel name or source offset is required.
- **Named:** opens a fresh demonstration channel, appends rows with offsets,
  waits for the final committed offset, reports channel status, and closes.
  The timestamp field demonstrates an additional column mapping.

## Errors and Delivery

Failures surface to the caller and resources are closed. The Elastic example does
not impose a short timeout on individual acknowledgements. Its 30-second close
budget is a cleanup limit. The named example has a 30-second commit wait; expiration
does not cancel ingestion or prove failure. Do not blindly replay after a timeout.

Elastic acknowledgements confirm durable buffering, not immediate table visibility
or successful materialization of every row. Check table contents and error logging.
Elastic replay can duplicate data. Keep real source events recoverable until confirmed.

The named quickstart uses a new channel name per run for a clean demonstration.
It is not a restart-recovery or exactly-once source integration recipe. Production
recovery requires stable channel ownership and source replay using the committed
offset returned on reopen. Neither quickstart implements crash recovery.

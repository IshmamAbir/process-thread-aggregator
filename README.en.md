# concurrent-counter

[日本語](README.md) | [English](README.en.md)

Concurrent number aggregator challenge. It uses only the Go standard library.

## Requirements

- A terminal such as Bash, PowerShell, or Command Prompt
- Go 1.22 or newer when running from source: <https://go.dev/dl/>
- Git, if you obtain the source by cloning the repository

If you plan to run from source, confirm that Go is available:

```text
go version
```

The command should report Go 1.22 or a newer version. This project uses only the Go standard library, so you do not need to install packages or start external services.

## How to run

### Contents

1. [Option 1: Run from source](#run-from-source)
2. [Option 2: Download a release](#download-release)
3. [Option 3: Run with Docker](#run-with-docker)

<a id="run-from-source"></a>

### Option 1: Run from source

Follow these steps from the computer where you want to run the program.

#### Step 1: Install Go

Install Go 1.22 or newer from <https://go.dev/dl/>. Close and reopen your terminal after installation.

Run this command:

```text
go version
```

Continue when the output shows Go 1.22 or newer, for example:

```text
go version go1.22.0 windows/amd64
```

<a id="download-source"></a>

#### Step 2: Download the source

Choose either the browser download or Git clone option. Both provide the same source files.

##### Option A: Download from GitHub without Git

1. Open <https://github.com/IshmamAbir/process-thread-aggregator> in a web browser.
2. Click the green **Code** button above the file list.
3. Click **Download ZIP** in the menu.
4. Wait for the browser to download `process-thread-aggregator-main.zip` or a ZIP file with a similar branch name.
5. Extract the downloaded ZIP:

   - On Windows, right-click the ZIP file, select **Extract All**, choose a destination, and click **Extract**.
   - On macOS, double-click the ZIP file.
   - On Linux, use your file manager's extract command or run `unzip process-thread-aggregator-main.zip`.

6. Open the extracted `process-thread-aggregator-main` folder. The exact suffix may match the branch that GitHub downloaded.
7. Open a terminal in that folder.

##### Option B: Clone with Git

Run these commands in PowerShell, Command Prompt, or a terminal:

```text
git clone https://github.com/IshmamAbir/process-thread-aggregator.git
cd process-thread-aggregator
```

Git creates the `process-thread-aggregator` folder and downloads the repository into it. The `cd` command moves your terminal into that folder.


#### Step 3: Confirm the source files

On Windows PowerShell, run:

```powershell
Get-ChildItem
```

On Linux or macOS, run:

```bash
ls
```

Confirm that the output includes these files:

```text
go.mod
main.go
main_test.go
README.md
README.en.md
```

#### Step 4: Run continuously

Run the same command on Windows, Linux, or macOS:

```text
go run . -m 4 -n 2
```

The command uses these settings:

- `-m 4` starts four generator threads inside each producer process.
- `-n 2` starts two producer processes. The parent aggregator makes the total process count three.
- Omitting `-run-for` keeps the program running until you stop it.

Leave the command running while you check the output in Step 5.

#### Step 5: Check the output

Wait for the first complete one-second interval. The terminal should print one JSON object per line in this form:

```json
{"time":"1788223061","counts":{"0":8,"1":3,"2":6,"3":14,"4":7,"5":13,"6":15,"7":11,"8":11,"9":11}}
```

Check the following fields:

- `time` contains a Unix timestamp as a string.
- `counts` contains the ten keys `0` through `9`.
- Each count is a non-negative integer.

The counts vary between runs because the threads generate random numbers. After you see at least two JSON records, press `Ctrl+C` once to stop the program. The parent sends a stop command to each producer, waits for them to exit, and kills any producer that remains after two seconds.

#### Step 6: Run a five-second example

Add `-run-for 5s` when you want the program to stop after five seconds:

```text
go run . -m 2 -n 2 -run-for 5s
```

This command starts two generator threads in each of two producer processes. It prints completed one-second records, stops after five seconds, cleans up the producer processes, and returns control to the terminal.

#### Step 7: Run with different process and thread counts

Replace the `-m` and `-n` values as needed. This example starts four producer processes with eight generator threads in each producer:

```text
go run . -m 8 -n 4 -run-for 10s
```

Both values must be greater than zero. Larger values create more concurrent work and produce more numbers per interval.

#### Step 8: Run the tests

Run the standard test suite:

```text
go test ./...
```

Run the race detector for the shared-map and goroutine code:

```text
go test -race ./...
```

The race detector requires CGO and a supported C compiler. Windows users can run this check in WSL or install a C compiler and set `CGO_ENABLED=1`.

#### Step 9: Build a reusable executable

You can use `go run .` for evaluation without building an executable. Use the command for your platform to create one.

##### Linux and macOS

```text
go build -o concurrent-counter .
./concurrent-counter -m 2 -n 2 -run-for 5s
```

##### Windows PowerShell or Command Prompt

```text
go build -o concurrent-counter.exe .
.\concurrent-counter.exe -m 2 -n 2 -run-for 5s
```

The program starts the producer processes from the same executable. Keep the executable in place until the run finishes.

<a id="download-release"></a>

### Option 2: Download a release

Download a compiled package from the [latest release](https://github.com/IshmamAbir/process-thread-aggregator/releases/latest) if you do not want to install Go.

| Operating system | CPU | Package suffix |
| --- | --- | --- |
| macOS | Apple silicon | `darwin_arm64.tar.gz` |
| macOS | Intel | `darwin_amd64.tar.gz` |
| Windows | Intel/AMD 64-bit | `windows_amd64.zip` |
| Windows | ARM 64-bit | `windows_arm64.zip` |
| Linux | Intel/AMD 64-bit | `linux_amd64.tar.gz` |
| Linux | ARM 64-bit | `linux_arm64.tar.gz` |

Extract the package, then run the executable:

```text
# Linux or macOS
./concurrent-counter -m 2 -n 2 -run-for 5s

# Windows
.\concurrent-counter.exe -m 2 -n 2 -run-for 5s
```

The release also contains `checksums.txt` with the SHA-256 digest of each package.

<a id="run-with-docker"></a>

### Option 3: Run with Docker

First, [download the source](#download-source) and open a terminal in the downloaded repository directory. The linked step covers GitHub ZIP download and Git clone.

With Docker installed and running, build the image from the repository directory:

```text
docker build -t concurrent-counter .
```

Run the program continuously, then press `Ctrl+C` to stop it:

```text
docker run --rm concurrent-counter -m 2 -n 2
```

Run the program for five seconds:

```text
docker run --rm concurrent-counter -m 2 -n 2 -run-for 5s
```

The options after the image name are the same command-line options described below. `--rm` removes the stopped container.

## Command-line options

Display the available command-line options without starting producer processes:

```text
go run . -h
```

Go's standard flag parser accepts both `-h` and `-help`. Both commands print the same usage information and exit:

```text
go run . -help
```

Use the help flag with a compiled executable in the same way:

```text
# Linux or macOS
./concurrent-counter -h

# Windows
.\concurrent-counter.exe -h
```

Use the help flags with Docker in the same way:

```text
docker run --rm concurrent-counter -h
docker run --rm concurrent-counter -help
```

| Option | Meaning | Default |
| --- | --- | --- |
| `-h`, `-help` | Print the available options and exit without running the program. | N/A |
| `-m` | Number of generator threads in each producer process. The value must be greater than zero. | `4` |
| `-n` | Number of producer processes. The aggregator runs as one additional process, giving `n + 1` processes in total. The value must be greater than zero. | `2` |
| `-run-for` | Optional run duration in Go duration syntax, such as `500ms`, `5s`, or `1m`. A value of `0` runs until `Ctrl+C`. Negative values are rejected. | `0` |

Examples:

```text
# One producer with one generator thread, running for 3 seconds
go run . -m 1 -n 1 -run-for 3s

# Four producers with eight generator threads each, running continuously
go run . -m 8 -n 4
```

## Output

The program writes one JSON object per completed Unix-second interval to standard output:

```json
{"time":"1788223061","counts":{"0":8,"1":3,"2":6,"3":14,"4":7,"5":13,"6":15,"7":11,"8":11,"9":11}}
```

`time` identifies the interval `[time, time + 1)`. `counts` contains all keys from `0` through `9`, including keys whose count is zero. Scheduling can change the number of generated values, so do not expect exactly 100 values per thread per second.

The program waits for a complete second before printing its first record. It discards the partial interval before the synchronized start and the partial interval at shutdown. A run shorter than one second may print no JSON records. The program writes diagnostics and input errors to standard error, leaving standard output suitable for JSON processing or file redirection.

## Run the checks

Run these commands from the repository directory:

```text
gofmt -l .
go vet ./...
go test ./...
go test -race ./...
go build -o concurrent-counter .
```

`gofmt -l .` produces no output when all Go files use standard formatting. Each other command returns exit status zero when its check succeeds. The race detector needs CGO and a supported C compiler. On Windows, install a C compiler and set `CGO_ENABLED=1`, or run the race check in Linux or WSL.

The tests cover counter updates and second boundaries, concurrent access to the shared map, aggregation across producers, JSON encoding, invalid input, producer failures, shutdown cleanup, interrupts, and a complete multi-process run.

## Publish a GitHub release

The release workflow runs when you push a tag whose name starts with `v`. Create and push an annotated semantic-version tag from the commit you want to publish:

```text
git tag -a v1.0.0 -m "v1.0.0"
git push origin v1.0.0
```

GitHub Actions runs `go test ./...`, builds the six packages listed above, creates `checksums.txt`, and publishes a GitHub Release with generated release notes. The release description groups direct download links under Mac, Windows, and Linux, and each asset has a matching platform label. The test suite runs on Linux AMD64; Go cross-compiles the other five packages without running them on their target systems.

## Troubleshooting

- If `go` is not recognized, install Go and reopen the terminal so the installer can update `PATH`.
- If the command prints no output, use `-run-for 2s` or longer. The program prints only completed whole-second intervals.
- If a flag fails validation, use positive values for `-m` and `-n`, and use a non-negative Go duration for `-run-for`.
- If `go test -race ./...` reports that it requires CGO, install a C compiler or run that check in Linux or WSL.

## Design and behavior

The command is the aggregator process. It starts N producer processes from the same executable, so the program has N producers plus one aggregator (N + 1 processes total). Each producer starts M generator goroutines, pins each one to an OS thread with `runtime.LockOSThread`, and generates an integer from 0 through 9 every 10 milliseconds (0.01 second).

Each producer stores per-second `[10]uint64` counts in a plain `map[int64][10]uint64`. One `sync.Mutex` per producer protects generation-time capture, map updates, and snapshots. The aggregator owns its state in one event loop and does not need another mutex.

The parent and each child communicate through anonymous OS pipes only: the parent sends synchronized `start` and `stop` commands over the child's stdin, and the child sends newline-delimited JSON frames over stdout. The parent knows which pipe belongs to which producer, combines all N frames for a second, and emits one JSON record after every producer has contributed.

All producers start on the same whole-second boundary. Completed idle seconds are still emitted with all ten counts set to zero. The initial pre-start partial second and the partial second in progress at shutdown are discarded. Consequently, a timed run shorter than one second can produce no records.

Standard output is reserved for newline-delimited result JSON. Help, validation errors, child diagnostics, and runtime errors go to standard error, so output can be redirected or piped safely.

The parent fails fast if any producer cannot start, emits invalid or nonconsecutive data, exits early, or fails during shutdown. It closes the other producers' control pipes, waits up to two seconds, then kills any remaining children. Producers are not restarted.

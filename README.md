# Producer-Consumer Synchronization and Deadlock Simulation

GitHub Repository: https://github.com/BehknQux/OS_Assignment

This project is a C implementation of a producer-consumer system using POSIX threads, mutexes, condition variables, bounded buffers, runtime configuration files, performance metrics, and deadlock detection.

The program simulates multiple producer and consumer threads working on one or more shared buffers. Different configuration files are used to test low load, high load, bottleneck, circular dependency, and deadlock-focused scenarios.

## Project Goals

The main goals of the project are:

- Implement the producer-consumer problem with bounded buffers.
- Use mutexes and condition variables for synchronization.
- Support multiple producers, multiple consumers, and multiple buffers.
- Read the experiment setup from external configuration files.
- Measure performance values such as produced item count, consumed item count, throughput, waiting time, CPU utilization, and deadlock statistics.
- Detect deadlock-like circular wait situations with a separate monitor thread.
- Provide different test cases for comparing system behavior under different loads.

## Folder Structure

```text
OS_Assignment/
|-- Makefile
|-- README.md
|-- docs/
|   |-- REPORT.pdf
|-- src/
|   |-- main.c
|   |-- system.h
|   |-- producer.c
|   |-- producer.h
|   |-- consumer.c
|   |-- consumer.h
|   |-- buffer.c
|   |-- buffer.h
|   |-- config.c
|   |-- config.h
|   |-- metrics.c
|   |-- metrics.h
|   |-- deadlock.c
|   |-- deadlock.h
|   |-- common/
|   |   |-- logger.c
|   |   |-- logger.h
|   |   |-- utils.c
|   |   |-- utils.h
|   |-- configs/
|       |-- low_load.conf
|       |-- high_load.conf
|       |-- bottleneck.conf
|       |-- circular_dependency.conf
|       |-- deadlock.conf
```

## Main Source Files

### `main.c`

`main.c` is the entry point of the program. It is responsible for:

- Selecting the configuration file.
- Loading and validating the configuration.
- Initializing the simulation structure.
- Creating producer, consumer, and monitor threads.
- Waiting until the configured runtime finishes or a deadlock stop request is received.
- Stopping all threads safely.
- Joining all created threads.
- Printing the final performance metrics.
- Releasing allocated memory and synchronization objects.

The main thread acts like the experiment controller. It does not directly produce or consume items. Instead, it starts the worker threads and controls when the experiment ends.

### `producer.c`

`producer.c` contains the producer thread logic. Each producer:

- Sleeps for its configured interval.
- Creates a new item.
- Waits if its output buffer is full.
- Inserts the item into the output buffer.
- Signals consumers waiting on the same buffer.
- Updates production metrics.

In deadlock-focused configurations, producers also use auxiliary locks. Producers acquire `resource_a` first and then try to acquire `resource_b`.

### `consumer.c`

`consumer.c` contains the consumer thread logic. Each consumer:

- Sleeps for its configured interval.
- Waits if its input buffer is empty.
- Removes an item from the input buffer.
- Updates consumption metrics.
- Optionally forwards a new item to another output buffer if it is an intermediate consumer.
- Signals producers or consumers waiting on the affected buffer.

In deadlock-focused configurations, consumers acquire auxiliary locks in the opposite order from producers. Consumers acquire `resource_b` first and then try to acquire `resource_a`.

### `buffer.c`

`buffer.c` implements bounded buffer operations. Each buffer stores integer items and has:

- A fixed capacity.
- A current item count.
- A mutex.
- A `not_full` condition variable.
- A `not_empty` condition variable.

The actual waiting logic is handled in producer and consumer code, while `buffer.c` provides simple insert/remove helper functions.

### `config.c`

`config.c` reads configuration files from `src/configs`. The project supports a compact assignment-style format such as:

```text
A[5]
t:60
P1>A
A>C1
P1:20
C1:20
```

It also accepts several key-value settings such as:

```text
deadlock_start_delay_ms=0
stop_on_deadlock=false
aux_lock_hold_ms=120
log_to_file=true
log_file=deadlock_watch.log
```

### `deadlock.c`

`deadlock.c` contains the deadlock monitor thread. This monitor periodically checks thread states and builds a wait-for graph from the current waiting information. If it finds a cycle in this graph, it reports a deadlock event and updates the deadlock metrics.

### `metrics.c`

`metrics.c` collects and prints experiment results:

- Total produced item count.
- Total consumed item count.
- Throughput.
- Average waiting time.
- Producer blocking time.
- Consumer blocking time.
- CPU utilization.
- Deadlock detection count.
- Deadlock frequency.

### `logger.c`

`logger.c` prints timestamped logs to the terminal. If `log_to_file=true`, it also writes logs to the configured log file.

## Build Instructions

The project uses a `Makefile`.

### Windows with MinGW

```bash
mingw32-make
```

This creates:

```text
pc_system.exe
```

To clean generated files:

```bash
mingw32-make clean
```

### Linux or macOS

```bash
make
```

This creates:

```text
pc_system
```

To clean generated files:

```bash
make clean
```

## Running the Program

If no config path is given, the program uses:

```text
src/configs/low_load.conf
```

Run default configuration:

```bash
./pc_system
```

On Windows:

```bash
.\pc_system.exe
```

Run a specific configuration:

```bash
./pc_system src/configs/high_load.conf
```

On Windows:

```bash
.\pc_system.exe src\configs\high_load.conf
```

## Configuration Format

Configuration files define buffers, runtime, producers, consumers, thread intervals, logging options, and optional deadlock settings.

### Buffer Definition

```text
A[5]
```

This creates buffer `A` with capacity `5`.

Multiple buffers can be defined:

```text
A[5]
B[5]
```

### Runtime

```text
t:60
```

This means the experiment runs for 60 seconds unless it is stopped earlier by deadlock handling.

### Producer Definition

```text
P1>A
```

This means producer `P1` produces items into buffer `A`.

### Consumer Definition

```text
A>C1
```

This means consumer `C1` consumes items from buffer `A`.

### Pipeline Consumer Definition

```text
A>C1>B
```

This means consumer `C1` consumes from buffer `A` and then forwards a new item to buffer `B`.

This allows the project to model multi-stage producer-consumer pipelines.

### Producer Interval

```text
P1:20
```

This means producer `P1` waits 20 ms between production attempts.

### Consumer Interval

```text
C1:20
```

This means consumer `C1` waits 20 ms between consumption attempts.

### Logging

```text
log_to_file=true
log_file=high_load.log
```

If `log_to_file=true`, logs are written to the terminal and also to the specified log file.

If `log_to_file=false`, logs are only printed to the terminal.

## Available Configurations

### `low_load.conf`

This is the basic low-load scenario.

It has:

- One buffer.
- Two producers.
- Two consumers.
- A moderate buffer size.
- No deadlock simulation.

This configuration is useful for showing normal producer-consumer behavior.

### `high_load.conf`

This scenario creates a heavier workload.

It has:

- One buffer.
- Ten producers.
- Ten consumers.
- Short producer and consumer intervals.
- File logging enabled.
- No deadlock simulation.

This configuration is useful for comparing throughput and waiting time under high contention.

### `bottleneck.conf`

This scenario is designed to create an imbalance between producers and consumers.

It has:

- Fewer producers.
- More consumers.
- Different producer and consumer intervals.
- A small buffer.

This configuration is useful for observing bottleneck behavior and waiting times.

### `circular_dependency.conf`

This configuration uses multiple buffers and consumers that forward items between buffers.

It is useful for testing pipeline-style dependencies and observing how the wait-for graph behaves in a more complex topology.

### `deadlock.conf`

This configuration is focused on deadlock simulation and detection.

It uses:

- Multiple buffers.
- Producers and consumers connected in a circular pipeline.
- Auxiliary locks.
- Deadlock monitoring.
- Deadlock metrics.

Important deadlock-related settings are included in this file:

```text
deadlock_start_delay_ms=0
stop_on_deadlock=false
aux_lock_hold_ms=120
```

## Deadlock Simulation

Deadlock simulation is controlled by `deadlock_start_delay_ms`.

If a configuration file does not contain `deadlock_start_delay_ms`, the auxiliary-lock deadlock mechanism stays disabled. This prevents normal load tests such as `high_load.conf` from producing artificial deadlock events.

If a configuration file contains:

```text
deadlock_start_delay_ms=0
```

then the auxiliary-lock deadlock mechanism starts immediately.

If it contains:

```text
deadlock_start_delay_ms=3000
```

then the simulation runs normally for 3000 ms first, and the auxiliary-lock mechanism starts after that delay.

## Auxiliary Locks

The project uses two auxiliary locks for deadlock simulation:

- `resource_a`
- `resource_b`

Producers acquire locks in this order:

```text
resource_a -> resource_b
```

Consumers acquire locks in this order:

```text
resource_b -> resource_a
```

This opposite ordering is intentional. Under contention, one producer may hold `resource_a` while waiting for `resource_b`, and one consumer may hold `resource_b` while waiting for `resource_a`. This creates the circular wait condition required for deadlock.

## `aux_lock_hold_ms`

```text
aux_lock_hold_ms=120
```

This value controls how long a thread holds the first auxiliary lock before trying to acquire the second one.

For example:

- A producer locks `resource_a`.
- It waits for `aux_lock_hold_ms`.
- Then it tries to lock `resource_b`.

The same idea applies to consumers, but in the opposite order.

A larger value increases the chance that producer and consumer threads overlap while holding different locks. Therefore, it increases the probability of observing a circular wait.

If `aux_lock_hold_ms=0`, a deadlock is still theoretically possible, but it becomes much harder to observe because a thread may acquire and release both locks very quickly.

## Deadlock Detection

Deadlock detection is implemented with a dedicated monitor thread.

The monitor periodically inspects all producer and consumer thread states. It checks whether threads are running or waiting. Waiting information is collected from the shared simulation state.

The implementation uses an internal timeout threshold before including a waiting thread in deadlock analysis. This avoids treating very short waits as deadlocks. This timeout is intentionally kept inside the code instead of being exposed as a configuration option.

After collecting waiting information, the monitor builds a wait-for graph.

In the wait-for graph:

- Each node represents a thread.
- A directed edge means one thread is waiting for a resource or condition related to another thread.

After the graph is built, DFS-based cycle detection is applied.

If a cycle is found:

- The cycle is logged.
- Deadlock metrics are updated.
- The program may stop immediately depending on `stop_on_deadlock`.

Example deadlock log:

```text
[ERROR] [DEADLOCK] Deadlock detected: P4 -> C3 -> P4
```

This means `P4` and `C3` are part of a circular wait.

## `stop_on_deadlock`

```text
stop_on_deadlock=true
```

If this setting is enabled, the simulation stops when a deadlock is detected. The monitor requests all worker threads to stop, the main thread joins them, and final metrics are printed.

```text
stop_on_deadlock=false
```

If this setting is disabled, the program reports the deadlock but continues running until the configured runtime ends.

This is useful when the experiment should collect metrics for the full duration even after detecting a deadlock event.

## Performance Metrics

At the end of the simulation, the program prints a metrics summary.

Example:

```text
=== Performance Metrics ===
Runtime                : 60.00 sec
Total produced         : 5000
Total consumed         : 4980
Throughput             : 83.00 items/sec
Average waiting time   : 2.40 ms
Producer blocking time : 120 ms
Consumer blocking time : 340 ms
CPU utilization        : 3.50 %
Deadlock detections    : 0
Deadlock frequency     : 0.0000 /sec
```

### Runtime

Total measured runtime of the experiment.

### Total Produced

The total number of items inserted into buffers by producers or forwarding consumers.

### Total Consumed

The total number of items removed from buffers by consumers.

`Total produced` and `Total consumed` do not always have to be equal. When the simulation stops, some items may still be waiting in buffers or may be in the middle of a pipeline stage.

### Throughput

The number of consumed items per second.

### Average Waiting Time

Average waiting time for buffer-related blocking events.

### Producer Blocking Time

Total time producers spent waiting because their output buffer was full.

### Consumer Blocking Time

Total time consumers spent waiting because their input buffer was empty or their output buffer was full.

### CPU Utilization

Approximate CPU usage of the process during the experiment.

### Deadlock Detections

The number of deadlock cycles detected by the monitor.

### Deadlock Frequency

Deadlock detections divided by runtime.

## Why Produced and Consumed Can Be Different

The project can use pipeline-style consumers. Some consumers remove an item from one buffer and then produce another item into another buffer.

Also, the program can stop while items are still inside buffers.

Therefore:

- `Total produced` means items that entered a buffer.
- `Total consumed` means items that were removed from a buffer.
- If items remain in buffers at the end, produced and consumed values can be different.

This is expected behavior, especially in high-load and deadlock-focused runs.

## Synchronization Design

The project uses several synchronization mechanisms:

- A mutex per buffer.
- `not_full` condition variable per buffer.
- `not_empty` condition variable per buffer.
- A shared simulation-state mutex.
- Auxiliary resource locks for deadlock simulation.

Condition variables are used so that:

- Producers wait when a buffer is full.
- Consumers wait when a buffer is empty.
- Threads wake each other when buffer state changes.

The simulation-state mutex protects monitoring information such as:

- Thread state.
- Waiting resource.
- Waiting buffer.
- Held auxiliary resources.
- Last progress time.
- In-flight pipeline items.

## Clean Shutdown

The program supports clean shutdown.

When runtime ends or deadlock stop is requested:

- A global stop flag is set.
- Buffer condition variables are broadcast.
- Sleeping or blocked threads wake up.
- Producer and consumer threads exit their loops.
- The main thread joins all created threads.
- Metrics are finalized and printed.
- Allocated memory is released.
- Mutexes and condition variables are destroyed.

This prevents worker threads from being left blocked on a condition variable after the main simulation ends.

## Example Commands

Build:

```bash
mingw32-make
```

Run low load:

```bash
.\pc_system.exe src\configs\low_load.conf
```

Run high load:

```bash
.\pc_system.exe src\configs\high_load.conf
```

Run bottleneck:

```bash
.\pc_system.exe src\configs\bottleneck.conf
```

Run circular dependency:

```bash
.\pc_system.exe src\configs\circular_dependency.conf
```

Run deadlock scenario:

```bash
.\pc_system.exe src\configs\deadlock.conf
```

Clean:

```bash
mingw32-make clean
```

## Notes

- The project is written in C.
- POSIX threads are used for concurrency.
- On Windows, MinGW is used to compile the project.
- Normal configurations do not enable the auxiliary-lock deadlock mechanism unless `deadlock_start_delay_ms` is explicitly present.
- The deadlock timeout used by the monitor is internal to the code, not a config-file parameter.
- High-load runs can produce many log lines, especially when terminal logging is enabled.
- For video demonstration, shorter runtimes can be used by changing `t:60` to a smaller value such as `t:20` or `t:30`.

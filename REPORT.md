# Operating Systems Course Project Report

## 1. Introduction

This project implements a multithreaded producer-consumer system for the Operating Systems course project. The system is designed to demonstrate concurrent execution, synchronization correctness, bounded-buffer communication, configurable task pipelines, deadlock detection, logging, and performance measurement.

The main goal of the project is to transform the classical producer-consumer problem into a more realistic concurrent processing model. Instead of using only a single producer and a single consumer, the system supports multiple producers, multiple consumers, named buffers, and pipeline workers that both consume from one buffer and produce to another. This makes the project suitable for modeling task queues, staged processing pipelines, and resource coordination scenarios.

The implementation is written in C using POSIX threads. Synchronization is achieved with mutexes and condition variables. Deadlock analysis is supported by a dedicated monitoring thread that observes thread states and periodically searches for cycles in a wait-for graph. In addition, the system records runtime metrics such as throughput, average waiting time, blocking times, deadlock detection count, deadlock frequency, and CPU utilization.

The project was designed to satisfy the requirements listed in the assignment document:

- concurrent producer and consumer threads
- bounded shared buffer structure
- synchronization with mutex-based design
- deadlock detection support
- configuration file parser
- logging output
- performance experiments under different workloads

## 2. Design Approach

The system is organized as a set of focused modules. Each module is responsible for one core concern, which improves readability, maintainability, and testing.

### 2.1 High-Level Architecture

The project uses the following main modules:

- `main.c`: program entry point, simulation setup, thread creation, experiment lifetime control, shutdown, and metric printing
- `config.c` / `config.h`: parses the configuration file and validates producer-consumer connections
- `buffer.c` / `buffer.h`: implements the bounded buffer queue abstraction
- `producer.c`: producer thread logic
- `consumer.c`: consumer and pipeline worker logic
- `deadlock.c`: deadlock monitoring and cycle detection
- `metrics.c` / `metrics.h`: collects and prints performance measurements
- `src/common/logger.c` / `logger.h`: thread-safe logging support
- `src/common/utils.c` / `utils.h`: timing, parsing, and utility helpers
- `system.h`: central runtime model shared by producer, consumer, main, and deadlock modules

### 2.2 Runtime Model

The simulation is represented by a shared `simulation_t` structure. It stores:

- all shared buffers
- loaded configuration
- logger instance
- metrics instance
- global state mutex
- auxiliary resources used in artificial deadlock experiments
- thread monitor entries used by the deadlock detector
- stop flag for clean shutdown

Each worker thread receives a `thread_context_t` structure. This context contains the thread's logical identity, timing configuration, related input/output buffers, and runtime counters.

This design keeps the worker code generic. Producer and consumer functions do not depend on hardcoded buffer names or thread counts. Instead, all runtime behavior comes from configuration.

### 2.3 Named Buffers and Pipelines

The project supports multiple named buffers such as `A` and `B`. Each producer writes to one output buffer. Each consumer reads from one input buffer. Some consumers may optionally produce into a second output buffer, which allows the construction of multi-stage pipelines.

For example:

```text
P1>A
A>C1>B
B>C3>A
A>C9
```

This configuration means:

- `P1` produces into buffer `A`
- `C1` consumes from `A` and produces into `B`
- `C3` consumes from `B` and produces into `A`
- `C9` only consumes from `A`

This pipeline model makes the system more expressive than a minimal textbook producer-consumer example.

### 2.4 Clean Shutdown Strategy

The system runs for a configured amount of time. When the time expires, the main thread sets a stop flag and broadcasts all buffer condition variables. This allows blocked threads to wake up, observe the shutdown condition, and exit cleanly. The project does not terminate threads abruptly.

## 3. Synchronization Design

Synchronization is implemented with a mutex-based design pattern and condition variables.

### 3.1 Why Mutex + Condition Variable

The assignment requires using at least one of Mutex or Semaphore design patterns. This project uses mutexes as the primary synchronization primitive and condition variables for waiting and wake-up coordination.

This design is suitable for the bounded-buffer problem because it separates two concerns:

- mutual exclusion for accessing shared buffer state
- orderly waiting when the buffer is full or empty

### 3.2 Per-Buffer Synchronization

Each `buffer_t` contains:

- queue storage for integer items
- `pthread_mutex_t mutex`
- `pthread_cond_t not_full`
- `pthread_cond_t not_empty`

This means every buffer has its own lock and condition variables. As a result, synchronization is fine-grained rather than global. Threads that operate on different buffers do not block each other unnecessarily.

### 3.3 Producer Synchronization Logic

The producer logic follows this pattern:

1. wait for its configured production interval
2. lock the output buffer mutex
3. if the buffer is full, wait on `not_full`
4. insert a produced integer into the queue
5. signal `not_empty` to wake a consumer
6. unlock the buffer mutex

This satisfies the classical producer rule: producers must wait when the buffer is full.

### 3.4 Consumer Synchronization Logic

The consumer logic follows this pattern:

1. wait for its configured processing interval
2. lock the input buffer mutex
3. if the buffer is empty, wait on `not_empty`
4. remove an item from the queue
5. signal `not_full` to wake a producer
6. unlock the input buffer mutex

If the consumer is a pipeline worker, it then:

1. locks the output buffer
2. waits on `not_full` if the output buffer is full
3. inserts a transformed/generated item into the output buffer
4. signals `not_empty`
5. unlocks the output buffer

This satisfies the classical consumer rule: consumers must wait when the buffer is empty.

### 3.5 Mutual Exclusion and Critical Sections

The protected critical data includes:

- `buffer->data`
- `buffer->count`
- `buffer->head`
- `buffer->tail`

Only one thread can modify a specific buffer's internal state at a time. This prevents race conditions such as:

- two producers writing into the same slot
- two consumers removing the same item
- inconsistent queue counters

### 3.6 Synchronization Logging

The assignment explicitly asks for synchronization logging. The system logs:

- lock acquisition events such as `Thread P1 has Lock R1`
- lock release events such as `Thread P1 released Lock R1`
- buffer lock acquisition and release
- condition signaling events such as `signaled not_empty on A`
- blocking events such as `Thread C2 waits because buffer A is empty`

This makes the synchronization behavior visible to the user and improves debuggability.

## 4. Deadlock Detection Method

Deadlock support is implemented as a monitoring module rather than as prevention-only logic.

### 4.1 Deadlock Model Used in the Project

The assignment requires detection of:

- circular wait
- blocked threads
- resource contention

The project supports these by tracking thread states in a shared monitor table. Each thread monitor entry stores:

- current state
- held auxiliary resources
- requested auxiliary resource
- waiting buffer index
- pipeline input/output information
- in-flight item state for pipeline transfers
- waiting start time

### 4.2 Wait-for Graph Approach

The assignment notes that `Wait-for graph` or `Resource Allocation Graph` can optionally be implemented. This project implements a wait-for graph.

The deadlock monitor thread periodically builds a graph where:

- nodes represent threads
- edges represent waiting relations

Two kinds of edges are created:

1. auxiliary resource wait edges
2. pipeline/buffer dependency edges

This allows the system to detect both classical resource deadlocks and project-specific pipeline wait cycles.

### 4.3 Auxiliary Resource Deadlock

For the artificial deadlock experiment, the system uses two auxiliary resources:

- `resource_a`
- `resource_b`

Producer threads attempt to acquire these in one order, while consumer threads attempt to acquire them in the reverse order. This intentionally creates the possibility of circular wait:

- a producer may hold `R1` and wait for `R2`
- a consumer may hold `R2` and wait for `R1`

This reproduces the standard deadlock pattern discussed in operating systems literature.

### 4.4 Pipeline Circular Dependency

The circular dependency experiment models a different kind of concurrency risk. In this setup:

- some consumers transform `A -> B`
- some consumers transform `B -> A`

This creates a pipeline dependency cycle. It does not always become a real deadlock, but it can significantly increase waiting times and can stall progress if conditions become unfavorable.

### 4.5 Detection Policy

The monitor thread does not treat every short wait as a deadlock. Instead, a thread becomes eligible for deadlock analysis only after exceeding a configured timeout. This avoids false positives from normal transient waits.

The detection policy is:

1. periodically wake up
2. inspect monitored threads
3. select threads that have been waiting longer than `deadlock_timeout_ms`
4. build the wait-for graph
5. run cycle detection
6. if a cycle is found, log it and update deadlock metrics

### 4.6 Reporting Behavior

When a deadlock is detected, the system:

- logs the cycle as an error
- increments the deadlock detection counter
- computes deadlock frequency at the end of execution

The program does not crash immediately when a deadlock is detected. Instead, it continues until the configured experiment duration ends, then shuts down cleanly. This makes the deadlock visible while preserving a usable experiment flow.

## 5. Experiments

The assignment defines five experiments. The project includes matching configuration files under `docs/configs/`.

### 5.1 Experiment 1: Low Load

Configuration summary:

- 2 producers
- 2 consumers
- producer interval: 10 ms
- consumer interval: 10 ms
- buffer: `A[6]`
- runtime: 60 seconds

Expected behavior:

- balanced production and consumption
- low blocking times
- no deadlock
- moderate throughput

Representative observation from testing:

- runtime about `60.51 sec`
- throughput around `124.99 items/sec`
- consumer blocking time remained small
- deadlock detection count stayed `0`

### 5.2 Experiment 2: High Load

Configuration summary:

- 10 producers
- 10 consumers
- producer interval: 10 ms
- consumer interval: 10 ms
- buffer: `A[20]`
- runtime: 60 seconds

Expected behavior:

- high throughput
- many fast state transitions
- increased contention on the shared buffer
- no deadlock in normal conditions

Representative observation from testing:

- runtime about `60.22 sec`
- throughput around `621.87 items/sec`
- deadlock detection count stayed `0`
- consumer blocking time increased compared to low load due to high competition

### 5.3 Experiment 3: Artificial Deadlock

Configuration summary:

- 4 producers
- 5 consumers
- buffers: `A[5]`, `B[5]`
- producer interval: 20 ms in the working configuration used during testing
- consumer interval: 20 ms in the working configuration used during testing
- runtime: 60 seconds
- circular wait simulation enabled

Expected behavior:

- system should demonstrate a lock-order deadlock scenario
- deadlock detector should report at least one cycle

Representative observation from testing:

- runtime about `60.54 sec`
- deadlock detection was triggered successfully
- deadlock detections `1`
- deadlock frequency became non-zero
- depending on scheduling, some runs produced items before the deadlock and some locked very early

### 5.4 Experiment 4: Bottleneck

Configuration summary:

- 4 producers
- 10 consumers
- producer interval: 50 ms
- consumer interval: 10 ms
- buffer: `A[5]`
- runtime: 60 seconds

Expected behavior:

- producer side becomes the throughput bottleneck
- consumers frequently wait on an empty buffer
- producer blocking time remains low

Representative observation from testing:

- runtime about `60.09 sec`
- throughput around `65.91 items/sec`
- consumer blocking time became very high
- producer blocking time stayed `0 ms`
- no deadlock was detected

### 5.5 Experiment 5: Circular Dependency

Configuration summary based on the assignment:

- 2 producers, both produce `A`
- 2 consumers transform `A -> B`
- 6 consumers transform `B -> A`
- 2 consumers only consume `A`
- buffers: `A[5]`, `B[5]`
- producer interval: 100 ms
- consumer interval: 10 ms
- runtime: 60 seconds

Expected behavior:

- no forced lock-order deadlock
- cyclic dependency in the pipeline
- high waiting times, especially on consumers
- possible slowdown or stall-like behavior depending on timing

Representative observation from testing:

- runtime about `60.44 sec`
- total produced `2592`
- total consumed `2592`
- throughput about `42.89 items/sec`
- average waiting time about `214.43 ms`
- consumer blocking time about `557949 ms`
- deadlock detections `0`
- deadlock frequency `0.0000 /sec`

This result is important: circular dependency did not necessarily become a formal deadlock, but it clearly increased system waiting costs.

## 6. Performance Evaluation

The performance section focuses on the metrics required by the assignment.

### 6.1 Throughput

Throughput is computed as:

```text
throughput = total_consumed / runtime_sec
```

This reflects how many completed consumption events the system processes per second. High load produced the highest throughput among the tested non-deadlock scenarios, while bottleneck and circular dependency produced lower throughput because they limit data availability or pipeline efficiency.

### 6.2 Average Waiting Time

Average waiting time summarizes the mean delay experienced during blocking events. In balanced scenarios this value stays small. In dependency-heavy or bottleneck scenarios it grows significantly.

Observed trend:

- low load: very low average waiting time
- high load: still moderate
- bottleneck: clearly higher
- circular dependency: high due to repeated waiting on empty or dependency-constrained buffers

### 6.3 Thread Blocking Times

The system records separate blocking totals for:

- producer blocking time
- consumer blocking time
- auxiliary lock waiting time

Interpretation:

- high producer blocking time means buffers are full and production cannot progress
- high consumer blocking time means buffers are empty or pipeline stages are waiting for upstream work
- high auxiliary lock waiting time indicates contention in the artificial deadlock path

In the bottleneck and circular dependency experiments, consumer blocking time was the dominant metric. This is consistent with the workload design.

### 6.4 Deadlock Frequency

The system reports both:

- `Deadlock detections`
- `Deadlock frequency`

Frequency is computed as:

```text
deadlock_frequency = deadlock_count / runtime_sec
```

This makes the deadlock metric easier to compare across experiments with different durations.

### 6.5 CPU Utilization

CPU utilization is included as an optional metric. The implementation estimates CPU utilization by comparing process CPU time against total wall-clock runtime and normalizing by the number of logical processors:

```text
cpu_utilization = (cpu_time / (runtime_sec * logical_processor_count)) * 100
```

On Windows, the process CPU time is read from `GetProcessTimes`, which avoids the coarse `clock()` behavior that can make the value look artificially pegged. This value is still an estimate, but it is much more useful for comparing experiment profiles.

### 6.6 Overall Evaluation of Experiments

The experiments show that the system reacts differently under different workloads:

- low load demonstrates stable and balanced producer-consumer execution
- high load increases throughput and contention
- artificial deadlock validates the deadlock detector
- bottleneck reveals producer-side throughput limitations
- circular dependency reveals how pipeline cycles can increase waiting time even without a forced deadlock

These results align with the course objectives related to synchronization, thread analysis, deadlock observation, and performance evaluation.

## 7. Conclusion

This project successfully implements a configurable multithreaded producer-consumer system with bounded buffers, synchronization control, deadlock monitoring, logging, and performance measurement.

The final design satisfies the core assignment requirements:

- multiple producer and consumer threads run concurrently
- bounded buffers support insertion, removal, full check, and empty check
- synchronization is implemented with a mutex-based design and condition variables
- deadlock monitoring detects circular wait situations through a wait-for graph
- configuration files define thread topology, timings, and buffer sizes
- logging displays production, consumption, synchronization, and deadlock events
- performance metrics summarize throughput, waiting times, blocking times, CPU utilization, and deadlock frequency

From an educational perspective, the project demonstrates several important operating systems concepts in a practical form:

- critical section protection
- coordination between concurrent threads
- staged data pipelines
- blocking vs. progress behavior
- deadlock observation and detection
- workload-sensitive performance analysis

The project can be extended in the future with more advanced recovery strategies, richer experiment automation, and external profiling support. However, in its current form, it already provides a complete and coherent implementation of the required operating systems course project.

# Concurrency / Multithreading Machine Coding Practice Set

Structured practice problems for machine coding interviews focused on concurrency and multithreading.  
Each problem starts with a base requirement and adds 3–4 progressive enhancements similar to real interview follow-ups.

---

## 1. Thread-Safe Bounded Blocking Queue

**Base:**
- Implement fixed-capacity queue with `put()` and `take()` that block when full/empty.

**Enhancements:**
- Support multiple producers and consumers
- Add timeout versions (`offer` with timeout, `poll` with timeout)
- Add fairness for waiting threads
- Add metrics (size, waiters, rejected ops)

---

## 2. Producer–Consumer Processing Pipeline

**Base:**
- Producer generates tasks, consumer processes them using a blocking queue.

**Enhancements:**
- Multi-stage pipeline (parse → transform → write)
- Backpressure when downstream is slow
- Graceful shutdown with poison pill / interrupt handling
- Retry with max retry count + dead letter queue

---

## 3. Job Scheduler with Dependencies (DAG Executor)

**Base:**
- Execute jobs respecting dependency order.

**Enhancements:**
- Run independent jobs in parallel
- Limit max worker threads
- Add priority-based scheduling
- Detect cycles and fail fast

---

## 4. Thread Pool / Task Executor

**Base:**
- Implement a fixed-size thread pool with `submit(task)`.

**Enhancements:**
- Bounded task queue + rejection policy
- Task priority support
- Dynamic thread scaling (min/max threads)
- Support scheduled/delayed tasks

---

## 5. Reader–Writer Key Value Store

**Base:**
- Thread-safe map with concurrent reads and writes.

**Enhancements:**
- Multiple readers, single writer (RW lock)
- Writer-priority vs reader-priority modes
- Snapshot reads (versioned data)
- TTL + background cleanup thread

---

## 6. Rate Limiter

**Base:**
- Allow N requests per time window.

**Enhancements:**
- Thread-safe under high concurrency
- Per-user + global limits
- Sliding window / token bucket
- Distributed design discussion (sharding/Redis)

---

## 7. Resource / Connection Pool

**Base:**
- Pool of reusable objects with acquire/release.

**Enhancements:**
- Max pool size with blocking acquire
- Timeout on acquire
- Idle eviction thread
- Leak detection + usage tracking

---

## 8. Concurrent LRU Cache

**Base:**
- Implement LRU cache with get/put.

**Enhancements:**
- Make thread-safe
- Reduce contention with lock striping
- Read-optimized path
- Async eviction / cleanup thread

---

## 9. Ordered Multithreaded Printing

**Base:**
- Two threads print odd/even numbers in order.

**Enhancements:**
- Extend to 3+ threads (FizzBuzz-style roles)
- Configurable number of threads
- Pause/resume printing
- Guarantee ordering under contention

---

## 10. Multithreaded File/Log Aggregator

**Base:**
- Multiple threads submit logs to a shared writer.

**Enhancements:**
- Batch writes by size threshold
- Time-based flush
- Async writer thread
- Crash-safe flush guarantees

---

## 11. Bounded Buffer Without Dynamic Allocation

**Base:**
- Fixed-size circular buffer using array only.

**Enhancements:**
- Make lock-based thread-safe
- Implement lock-free version
- Add wait-free reads if possible
- Add monitoring counters

---

## 12. Scheduled Job Runner

**Base:**
- Run tasks after a given delay.

**Enhancements:**
- Periodic tasks (fixed rate vs fixed delay)
- Priority by next execution time
- Cancellation support
- Persistence/recovery discussion

---

## 13. Multi-Reader Multi-Writer File Simulator

**Base:**
- Threads read/write shared file object.

**Enhancements:**
- Chunk-level locking
- Write batching
- Version conflict detection
- Rollback support

---

## 14. Fair Resource Allocator (Bathroom/Playground Style)

**Base:**
- Limited-capacity shared resource with rules.

**Enhancements:**
- Group-based access constraint
- Starvation prevention
- Fair queue ordering
- Priority override mode

---

## 15. Concurrent Metrics Collector

**Base:**
- Threads update counters/gauges.

**Enhancements:**
- Lock-free counters
- Percentile calculation window
- Snapshot export without blocking writers
- Rolling time windows

-------------------------------------

# Print N Numbers Using Different Even-Odd Threads
The goal is to print the numbers in order, while one thread only prints the even
numbers and the other thread only prints the odd numbers.

# Thread Safety in Singleton
Design a thread-safe singleton class in Java, ensuring that only a single
instance is created even in a multi-threaded environment.

# Parallel Matrix Multiplication
Implement a program that performs matrix multiplication using multiple threads
to achieve parallel processing and improve performance.

# Concurrent data structures
- Linked list
- Queue
- Stack

# Bathroom
- Fairness/starvation free
- Caste variation
- Multi bathroom variation
- Priority

# Hit Counter
# Rate Limiter
Implement a rate limiter that can control the rate of job execution, ensuring
that jobs are not executed more frequently than a specified limit.

# Thread-safe LRU Cache
Implement a thread-safe LRU (Least Recently Used) cache with support for
concurrent read and write operations, considering efficiency and thread safety.

# Web crawler
Deque
- Exit when disk is full.

# Concurrent Task Scheduler
Implement a concurrent task scheduler that can execute multiple tasks
concurrently while limiting the maximum number of parallel executions.
- void return type
- with some return value
- task delay support
- shutdown support
- cancel support
- cache support
- dont accept new tasks if some currently running task fails
- tasks with dependencies

# Thread-Safe Job Queue
Implement a thread-safe job queue in Java that supports adding jobs, removing
jobs, and retrieving the next job to execute efficiently.

# Priority Job Scheduler
Implement a job scheduler that supports job priorities and allows for
dynamically changing the priority of running jobs.

# Distributed Lock Manager
Design a distributed lock manager that can coordinate job execution across
multiple nodes to prevent concurrent execution of conflicting jobs.

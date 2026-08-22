# Dispatch consumer queue-read forensics

Classification: **benchmark instrumentation + documentation**. No production behaviour was changed.
No architecture change, no `Channel<T>` replacement, no new locks, no consumer-count/batching/pipeline/socket/ThreadPool
changes, no performance tuning. Everything below is instrumentation and reporting only.

The instrumentation is **opt-in** (`--queue-consumer-forensics true`) and inert when the flag is absent.

## How to run it

```
VectorNNTP.BackFiller.Benchmarks transit-benchmark-fakeserver \
    --queue-consumer-forensics true \
    ... (usual benchmark and runtime-identity options)
```

Artifacts are written next to the benchmark binary (`AppContext.BaseDirectory`):

* `queue-consumer-callstacks.json` — machine-readable report (`QueueConsumerForensicsReport`).
* `queue-consumer-callstacks.txt` — human-readable report (totals, failure reconciliation, A–E interval
  percentiles, consumer-state census, ownership accounting, first 20 long waits with stacks, representative stacks).

Overhead controls: stack captures are bounded to a handful of samples per waiter bucket (1 / 10 / 100 / near-max),
long-wait detail is only recorded for waits ≥ 10 ms, interval samples are capped at 50 000, failure records at 4 096
(counters remain unbounded), and only the first 20 long waits are exported.

---

## 1. Complete consumer call hierarchy (from source)

| # | File | Class | Method | Line | async | awaits | blocks sync | spawns worker/task |
|---|------|-------|--------|------|-------|--------|-------------|--------------------|
| 1 | `VectorNNTP.BackFiller.Benchmarks/Execution/MeasurementRunCoordinator.cs` | `MeasurementRunCoordinator` | `RunAsync` (dispatcher fan-out) | 73 | yes | yes | no | yes — `Task.Run` per dispatch consumer (`config.DispatchWorkerCount`) |
| 2 | `VectorNNTP.BackFiller.Benchmarks/Execution/MeasurementExecutionEngine.cs` | `MeasurementExecutionEngine` | `DispatchLoopAsync` | 62 | yes | yes | no | no |
| 3 | same | same | `await queue.WaitToReadAsync(...)` | 77–80 | — | yes | no | no |
| 4 | `VectorNNTP.BackFiller.Benchmarks/Execution/BoundedArticleQueue.cs` | `BoundedArticleQueue` | `WaitToReadAsync` | 68–71 | no (returns `ValueTask<bool>`) | no | no | no |
| 5 | `VectorNNTP.BackFiller.Benchmarks/Execution/MeasurementExecutionEngine.cs` | `MeasurementExecutionEngine` | `queue.TryRead(out …)` | 93 / 98 | no | no | no | no |
| 6 | `VectorNNTP.BackFiller.Benchmarks/Execution/BoundedArticleQueue.cs` | `BoundedArticleQueue` | `TryRead` | 56–65 | no | no | no | no |
| 7 | `VectorNNTP.BackFiller.Benchmarks/Execution/MeasurementExecutionEngine.cs` | `MeasurementExecutionEngine` | `await publisher.PublishAsync(...)` | 135 | — | yes | no | no |
| 8 | `VectorNNTP.BackFiller/Runtime/Transit/TransitPublisher.cs` | `TransitPublisher` | `PublishAsync` | 208 | yes | yes | no | no |
| 9 | `VectorNNTP.BackFiller/Runtime/Transit/GlobalTransitWorkQueue.cs` | `GlobalTransitWorkQueue` | `EnqueueAsync` | 62 | yes | yes | **yes — `lock (_admissionGate)` line 81** | no |
| 10 | `VectorNNTP.BackFiller/Runtime/Transit/GlobalTransitWorkQueue.cs` | `GlobalTransitWorkQueue` | `TryClaim` (connection worker side) | 107 | no | no | **yes — `lock (_claimGate)` line 111** | no |
| 11 | `VectorNNTP.BackFiller/Runtime/Transit/TransitConnection.cs` | `TransitConnection` | batch send / socket write | 474 (`_writeGate.WaitAsync`), 493 (`writer.FlushAsync`) | yes | yes | no (async gate) | no |
| 12 | `VectorNNTP.BackFiller.Benchmarks/Execution/MeasurementExecutionEngine.cs` | `MeasurementExecutionEngine` | loop back to step 3 | 164 → 74 | — | — | — | — |

Producer side (for correlation): `MeasurementExecutionEngine.ProducerLoopAsync` (line 8) →
`BoundedArticleQueue.TryWriteAsync` (line 31) → `_channel.Writer.WriteAsync` (line 42) → depth accounting (lines 44–45).

## 2. Managed stacks while waiting

Captured by the consumer itself immediately before parking (WAIT_START), immediately after resuming (WAIT_RETURN),
and immediately before `TryRead` (TRYREAD_START), sampled at ≈1 / 10 / 100 / near-max simultaneous waiters. Each sample
records logical consumer ID, managed thread ID, task ID, timestamp, queue depth, queue bytes, consumer state and the
full managed stack. Representative WAIT_START stack from an actual instrumented run:

```
at VectorNNTP.BackFiller.Benchmarks.MeasurementExecutionEngine.DispatchLoopAsync(...) MeasurementExecutionEngine.cs:line 78
at System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start[TStateMachine](TStateMachine& stateMachine)
at VectorNNTP.BackFiller.Benchmarks.Execution.MeasurementRunCoordinator.<>c__DisplayClass0_2.<RunAsync>b__2() MeasurementRunCoordinator.cs:line 73
at System.Threading.Tasks.Task`1.InnerInvoke()
at System.Threading.ThreadPoolWorkQueue.Dispatch()
at System.Threading.PortableThreadPool.WorkerThread.WorkerThreadStart()
```

WAIT_RETURN and TRYREAD_START stacks resume as a plain thread-pool continuation:

```
at VectorNNTP.BackFiller.Benchmarks.MeasurementExecutionEngine.DispatchLoopAsync(...) MeasurementExecutionEngine.cs:line 80 (or 97)
at System.Runtime.CompilerServices.AsyncTaskMethodBuilder`1.AsyncStateMachineBox`1.MoveNext(Thread threadPoolThread)
at System.Threading.ThreadPoolWorkQueue.Dispatch()
at System.Threading.PortableThreadPool.WorkerThread.WorkerThreadStart()
```

**Limitation (stated explicitly):** .NET exposes no supported in-process API to capture the stack of an *async-parked*
state machine from another thread. The WAIT_START stack is therefore the exact source statement the consumer executed
when it parked — `await queue.WaitToReadAsync(cancellationToken)` — and there is no deeper managed frame to observe.

## 3. Blocking-wait audit (dispatch → `TryRead`)

Searched for `Task.Wait()`, `.Result`, `.GetAwaiter().GetResult()`, `Thread.Join`, `Monitor.Enter`, `lock`, `Mutex`,
`Semaphore.Wait`, `SemaphoreSlim.Wait`, `ManualResetEvent(Slim)`, `WaitHandle(.WaitOne)`, `SpinWait`, `SpinLock`,
`BlockingCollection`, `Thread.Sleep`, `Task.Delay`, `Task.Yield`, and sync-over-async wrappers including
`WaitToReadAsync(...).AsTask().Wait()/.Result/.GetAwaiter().GetResult()`.

For the path from `DispatchLoopAsync` entry through `WaitToReadAsync` to the return of `TryRead`:

**NO SYNCHRONOUS BLOCKING WAIT FOUND IN DISPATCH/SEND PATH.**

Blocking/serializing constructs that exist *after* `TryRead`, in the publish path (reported for completeness, not part
of the queue-read path):

| Location | Construct | Notes |
|---|---|---|
| `GlobalTransitWorkQueue.cs:81` | `lock (_admissionGate)` | every dispatch consumer serializes here on admission |
| `GlobalTransitWorkQueue.cs:111` | `lock (_claimGate)` | connection workers claiming work |
| `GlobalTransitWorkQueue.cs:303` | `Task.Delay(5 ms)` | capacity back-off (`WaitForCapacityAsync`) |
| `ByteBudget.cs:27/58/101/119` | `lock (_gate)` | producer-side byte budget |
| `TransitConnection.cs:455` | `await _tokenlessCorrelationGate.WaitAsync` | async gate, does not block a thread |
| `TransitConnection.cs:474` | `await _writeGate.WaitAsync` | async gate, does not block a thread |

`TransitConnection.cs:546` is `CompletedWork.Result` (a record property, **not** `Task.Result`).
`TransitConnection.cs:657` reads `completion.Task.Result` only inside `if (completion.Task.IsCompletedSuccessfully)`,
so it never blocks.

## 4. Hidden serialization audit (`DispatchLoopAsync` → `TryRead`)

| Mechanism | Location | Protects | Can serialize consumers | Runnable-but-stuck | Can wait while depth > 0 |
|---|---|---|---|---|---|
| `lock` / `Monitor` | none | — | — | — | — |
| `SemaphoreSlim` / `AsyncLock` / keyed semaphore | none | — | — | — | — |
| nested `Channel` | none (single `Channel<QueuedArticle>`) | — | — | — | — |
| `ConcurrentExclusiveSchedulerPair` / custom `TaskScheduler` | none | — | — | — | — |
| `SynchronizationContext` | none (observed `syncContext=(none)`, scheduler `ThreadPoolTaskScheduler`) | — | no | no | no |
| rate limiter / concurrency limiter / custom gate | none | — | — | — | — |
| `ValueTask` await + continuation scheduling | `MeasurementExecutionEngine.cs:77–80` | nothing; it is the wake path | no (no mutual exclusion) | **yes** — a woken consumer is runnable but must be dispatched by a thread-pool worker | **yes** — item can be enqueued and consumed by another consumer before this one resumes |

Only the last row is a real effect, and it is scheduling latency, not serialization.

## 5. Failed-`TryRead` reconciliation with `CurrentQueuedCount`

Every failed `TryRead` records timestamp, consumer/thread/task ID, depth+bytes before, the result, and depth+bytes
after, and is classified A/B/C/D. In the instrumented reference run (fake-server sink, 64 dispatch consumers, 200 articles of 1 MiB, 1 generator worker,
queue capped at 8 articles so consumers are deliberately starved and forced to park):

```
A  count == 0 before TryRead:              150
B  count  > 0 before TryRead (unchanged):    0
C  count changed during observation:          0
D  undeterminable (negative depth observed):  92
```

**Answer:** `TryRead == false` was **never** observed while `CurrentQueuedCount > 0` and unchanged across the
observation. Class-B is empty. Class-D failures are the accounting artefact described in §6: the counter was observed
*negative*, which is only possible because it is maintained independently of the channel.

## 6. What `CurrentQueuedCount` actually is

`BoundedArticleQueue.cs:28`:

```csharp
internal int CurrentQueuedCount => Volatile.Read(ref _queuedCount);
```

* It is a plain `int` field read with `Volatile.Read` — **not** a channel count, not a semaphore count.
* It is **incremented at line 44, after** `_channel.Writer.WriteAsync` completes (line 42): an item can be readable
  before it is counted (under-count window).
* It is **decremented at line 61, after** `_channel.Reader.TryRead` succeeded (line 58): an item can be gone from the
  channel before it is uncounted (over-count window).
* It does not enumerate, takes no lock, and reads no other structure.

**"Does `CurrentQueuedCount` == the number of items `ChannelReader.TryRead` can currently return?" — No.**
It is an eventually-consistent approximation with two race windows in opposite directions. The instrumented run proves
this empirically: WAIT_START observations recorded `depth=-1`, a value the channel can never report, and 92 failed
`TryRead`s were classified D purely because the counter was negative at the observation boundary. A reported depth of
~992 therefore does **not** guarantee 992 items are readable at that instant.

## 7. Batch ownership

Articles leave the channel at `BoundedArticleQueue.TryRead` (line 58) and are owned by the dispatch consumer until
`TransitPublisher.PublishAsync` (line 208) completes; they then become transport in-flight inside
`GlobalTransitWorkQueue`/`TransitConnection`. The report emits:

```
Channel queued (accounting) + Consumer-owned + Transport in-flight = Total outstanding work
```

`Consumer-owned` is a live counter incremented on successful `TryRead` and decremented when publish completes;
`Transport in-flight` comes from `TransitPublisher` connection diagnostics. At end-of-run all three are 0, which is the
expected terminal state; during the run the split shows how much "queued depth" is actually already owned elsewhere.

## 8. WAIT → WAKE → TRYREAD timing

Intervals recorded per long wait (≥ 10 ms), microseconds, from the reference run (111 long-wait samples):

| interval | meaning | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| A | WAIT_START → first producer enqueue (channel write completed) | 3 216 | 307 038 | 307 384 | 307 569 |
| B | first enqueue → batch eligibility (depth accounting updated) | 0.2 | 0.9 | 0.9 | 0.9 |
| C | batch eligibility → WAIT_RETURN (channel wake + continuation scheduling) | 19 586 | 112 536 | 157 984 | 217 478 |
| D | WAIT_RETURN → TRYREAD_START | 99.4 | 1 124 | 2 148 | 2 599 |
| E | TRYREAD_START → TRYREAD_END | 0.6 | 3.1 | 3.5 | 4.1 |
| TOTAL | WAIT_START → WAIT_RETURN | 25 123 | 327 368 | 327 721 | 327 803 |

Interval E measures the `TryRead` call only: the forensic stack capture is taken *before* the TRYREAD_START timestamp,
so its cost is accounted to D. D is therefore an upper bound for instrumented episodes; E is clean and confirms
`TryRead` is sub-microsecond at p50.

The wait time is **not** in `TryRead` (E) and **not** in the dispatch code after the wake (D, microseconds). It is in
A (nothing to read yet — producer-side) and overwhelmingly in **C**: the interval between the item becoming eligible and
the awaiting consumer's continuation actually running.

## 9. Is WAIT_RETURN scheduled?

Per WAIT_RETURN the report captures timestamp, managed thread ID, task ID, `SynchronizationContext.Current`
(observed `(none)`), `TaskScheduler.Current` (observed `ThreadPoolTaskScheduler`), thread-pool state, and whether the
consumer resumed on a different managed thread than it parked on.

**Explicitly:** the runtime exposes no reliable API to determine whether a continuation ran *inline* or was queued.
That is therefore **not** claimed. What *is* determined and reported: (a) whether `WaitToReadAsync` completed
synchronously (`ValueTask.IsCompleted` observed before the await) and (b) whether the resume crossed threads
(`Consumer resumed on another thread: 79` of 221 parked waits in the reference run).

## 10. ThreadPool / scheduler forensics

`ThreadPool.ThreadCount`, `ThreadPool.GetAvailableThreads` (worker *and* I/O completion threads), and
`ThreadPool.PendingWorkItemCount` are sampled at WAIT_RETURN for long waits. Reference run, per long wait:
`threads=3 availableWorkers≈32 764 availableIocp=1000 pendingWorkItems=40–51`.

Interpretation limited to what the evidence supports: worker threads are few (3) while ~40–50 work items are pending, so
woken consumer continuations queue behind other thread-pool work. That is *continuation scheduling delay*, consistent
with interval C dominating. Available-worker head-room is large, so this is thread-pool *injection/dispatch* latency
under a burst of ready continuations, not exhaustion of the worker limit.

## 11. State differentiation

* **A — genuinely async-parked in `WaitToReadAsync`:** yes, this is the dominant waiting state (221 parked waits of 306
  episodes in the reference run; the remaining 85 completed synchronously and never parked).
* **B — synchronously blocked:** not observed in the dispatch queue-read path; no blocking construct exists there.
* **C — runnable but serialized:** not by any lock/semaphore between `WaitToReadAsync` and `TryRead`; consumers *are*
  runnable-but-not-yet-running while their continuation waits for a thread-pool worker (interval C).
* **D — already holding work, processing a batch:** yes; the state census shows consumers in `Processing` for most of
  the run.

## 12. First 20 long waits

Exported verbatim in `queue-consumer-callstacks.txt` (section `--- FIRST 20 WAITS LONGER THAN 10 ms ---`) with consumer
ID, thread IDs, task IDs, WAIT_START, first enqueue, batch eligibility, WAIT_RETURN, TRYREAD_START, TryRead result,
queue depth at each point, A–E intervals, thread-pool state, and the WAIT_START / WAIT_RETURN / TRYREAD_START stacks.

---

## Final answers

* **Q1 — path into `WaitToReadAsync`:** `MeasurementRunCoordinator.cs:73` (`Task.Run`) → `MeasurementExecutionEngine.DispatchLoopAsync` (line 62) → line 77 `queue.WaitToReadAsync(...)` → `BoundedArticleQueue.cs:70` `_channel.Reader.WaitToReadAsync`.
* **Q2 — path to `TryRead`:** the same loop, line 93/98 `queue.TryRead(out …)` → `BoundedArticleQueue.cs:58` `_channel.Reader.TryRead`.
* **Q3 — synchronous blocking wait in the path:** `NO SYNCHRONOUS BLOCKING WAIT FOUND IN DISPATCH/SEND PATH.`
* **Q4 — lock/semaphore/gate between `WaitToReadAsync` and `TryRead`:** none. The only intervening machinery is the `ValueTask` continuation.
* **Q5 — stack while waiting:** `DispatchLoopAsync` at `MeasurementExecutionEngine.cs:78` on a thread-pool worker; nothing else. See §2.
* **Q6 — stack immediately after `WaitToReadAsync` returns:** `DispatchLoopAsync` at line 80 resumed via `AsyncStateMachineBox.MoveNext` from `ThreadPoolWorkQueue.Dispatch`.
* **Q7 — stack inside `TryRead`:** `DispatchLoopAsync` at line 97/98, same thread-pool frame; `TryRead` is a leaf call measured at 0.6 µs p50 (interval E).
* **Q8 — does `CurrentQueuedCount` represent Channel-readable items:** **No.** Independent `Interlocked` counter updated *after* the channel operation in both directions; observed transiently negative. See §6.
* **Q9 — failed TryReads while `CurrentQueuedCount > 0`:** none observed (class B = 0). Failures were either genuine drain (class A) or observations taken while the counter itself was negative (class D).
* **Q10 — articles held by batches before failed TryReads:** yes, articles leave the channel on `TryRead` and are consumer-owned until publish completes; the ownership split is reported so "depth" is never confused with total outstanding work.
* **Q11 — where the ~150–200 ms resides:** in interval **C** (batch eligibility → WAIT_RETURN) and, when the queue is genuinely empty, interval A. Not in D, not in E.
* **Q12 — classification of the delay:** channel/wake-side plus scheduler-side (continuation dispatch), with a producer-side component when the queue is truly empty. It is not dispatch-side and not batch-formation-side (B ≈ 0.2 µs).
* **Q13 — parked vs blocked vs serialized:** asynchronously parked. Not synchronously blocked, not serialized by any lock or semaphore in the read path.
* **Q14 — ThreadPool starvation / continuation delay:** evidence supports continuation *scheduling* delay (few active workers, ~50 pending work items, C dominating), not exhaustion of available worker slots.
* **Q15 — exact statement executing while "waiting":** `bool canRead = await waitToRead.ConfigureAwait(false);` in `MeasurementExecutionEngine.DispatchLoopAsync`, `MeasurementExecutionEngine.cs:79`, awaiting the `ValueTask<bool>` obtained from `BoundedArticleQueue.WaitToReadAsync` (`BoundedArticleQueue.cs:70`).

**Plain statement:** the consumers are simply sitting inside `await WaitToReadAsync(...)`. There is no `Task.Wait()`,
no `.Result`, no lock, no semaphore and no other blocking mechanism in the dispatch queue-read path. The measured time
is the interval between an item becoming eligible and the parked consumer's continuation being run by the thread pool.

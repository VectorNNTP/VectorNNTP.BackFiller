# Synthetic Channel wake-up final forensics

Classification: **benchmark + documentation only**. No production code, topology, batching, sockets, queue implementation, ThreadPool setting, or application benchmark was changed.

## Method

`channel-wakeup-forensic` is an isolated `Channel<SyntheticItem>` experiment. It has no application dependencies in its hot path. For every wave, all N async consumers have registered `WaitToReadAsync`; then one or four `Task.Run` producers synchronously `TryWrite` N timestamped items. Consumers timestamp immediately after the await, immediately before `TryRead`, and immediately after `TryRead`. There are no sleeps, delays, yields, locks, batching, sockets, or application accounting.

The collected C value is `producer timestamp immediately before successful TryWrite` to consumer continuation resumption. It is a conservative upper bound: the synchronous `TryWrite` itself is included. Warm-up was 10 waves; each topology has three independent measured trials of 100 waves. The raw JSON artifact is emitted as `synthetic-channel-wakeup-forensics.json`.

## Measured facts

Environment: .NET 8 benchmark target, Debug/x64/net8.0, Linux execution host. The Windows self-contained output cannot execute on the Linux host (`libhostpolicy.so` unavailable), so the identical source was restored, clean-built, and run for `linux-x64`. This is an execution-environment limitation, not a production result.

Each cell below is the range across the three trials. All trials had exactly `N * 100` successful reads, zero failed reads, a maximum waiter population exactly N, and maximum simultaneous `TryRead` of one in the measured interval.

| Consumers | Producers | C p50 µs | C p95 µs | C p99 µs | C max µs | Throughput/s |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 0.500–0.756 | 0.571–1.426 | 0.701–5.398 | 1.883–234.063 | 131,079–560,538 |
| 1 | 4 | 0.736–0.871 | 0.962–1.552 | 1.152–2.449 | 3.496–4.026 | 170,561–345,662 |
| 2 | 1 | 0.751–0.891 | 0.901–1.127 | 1.543–3.465 | 3.215–33.070 | 448,029–837,521 |
| 2 | 4 | 0.501–0.951 | 1.076–2.098 | 2.108–4.487 | 2.523–41.277 | 433,934–523,149 |
| 4 | 1 | 0.816–1.312 | 1.783–2.003 | 3.706–4.166 | 5.118–29.785 | 1,040,312–1,150,748 |
| 4 | 4 | 0.366–1.302 | 1.147–2.223 | 2.343–4.321 | 4.957–57.222 | 798,085–993,049 |
| 8 | 1 | 1.888–1.938 | 2.679–3.556 | 4.472–6.249 | 5.173–27.151 | 1,422,222–1,895,285 |
| 8 | 4 | 1.156–1.257 | 2.593–3.630 | 3.685–7.346 | 19.004–31.112 | 1,251,173–1,306,403 |
| 16 | 1 | 4.011–4.132 | 5.198–6.179 | 9.324–12.254 | 22.459–242.886 | 1,497,707–2,120,610 |
| 16 | 4 | 3.470–3.620 | 5.083–7.375 | 6.123–11.012 | 7.927–152.420 | 1,207,821–1,766,394 |
| 32 | 1 | 8.343–8.648 | 13.030–13.961 | 18.398–33.395 | 26.083–38.598 | 2,194,787–2,280,779 |
| 32 | 4 | 8.167–8.218 | 12.069–12.514 | 13.696–22.970 | 29.570–36.976 | 2,033,941–2,105,817 |
| 64 | 1 | 16.610–17.215 | 25.599–31.423 | 40.491–54.742 | 50.847–68.674 | 2,069,791–2,403,395 |
| 64 | 4 | 16.669–17.132 | 24.307–27.111 | 28.818–41.332 | 45.042–62.850 | 2,218,060–2,392,971 |
| 128 | 1 | 31.918–35.058 | 44.752–50.867 | 51.998–59.951 | 59.229–125.523 | 2,485,920–2,731,949 |
| 128 | 4 | 31.873–35.233 | 43.706–50.696 | 47.732–64.457 | 62.635–96.746 | 2,454,411–2,695,475 |
| 256 | 1 | 64.613–69.560 | 91.278–104.172 | 101.347–115.589 | 108.643–128.624 | 2,455,706–2,729,968 |
| 256 | 4 | 63.531–92.590 | 90.602–145.930 | 111.168–163.666 | 201.884–553.198 | 1,381,648–2,382,082 |
| 512 | 1 | 144.668–155.985 | 210.337–234.163 | 222.960–256.065 | 245.290–308.750 | 2,305,258–2,675,977 |
| 512 | 4 | 135.024–143.321 | 206.080–209.626 | 216.576–264.188 | 238.264–325.300 | 2,166,279–2,681,400 |

Resume-to-`TryRead` and `TryRead` remain microsecond-scale in the artifact; the latter is the narrow leaf call. ThreadPool maximum observed thread count was 2–3. Pending work grew with the simultaneous wake burst (1/4/8/16/32/64/128/256/512 consumers: maxima 0–1/1–5/3–7/8–11/16–19/32–35/64–67/128–131/256–259/512–515). Consumer migrations occurred in every multi-consumer topology (0–87 trial-level migrations), confirming queued asynchronous execution rather than affinity.

## Source-level mechanism

Runtime source: [`dotnet/runtime` v8.0.0](https://github.com/dotnet/runtime/tree/v8.0.0).

1. `UnboundedChannelReader.WaitToReadAsync` and `BoundedChannelReader.WaitToReadAsync` register an `AsyncOperation<bool>` in `_waitingReadersTail` under `SyncObj` (`UnboundedChannel.cs`, `BoundedChannel.cs`).
2. `UnboundedChannelWriter.TryWrite` / `BoundedChannelWriter.TryWrite` drain that list through `ChannelUtilities.WakeUpWaiters`.
3. `WakeUpWaiters` calls `AsyncOperation<bool>.TrySetResult(true)` for **every** waiting reader.
4. `AsyncOperation<TResult>.SignalCompletion` sees `_runContinuationsAsynchronously == true` because `AllowSynchronousContinuations` is false. Its exact asynchronous path is `UnsafeQueueSetCompletionAndInvokeContinuation()` in `AsyncOperation.netcoreapp.cs`, which calls `ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false)`.
5. The global ThreadPool work item executes `AsyncOperation<TResult>.IThreadPoolWorkItem.Execute`, then `SetCompletionAndInvokeContinuation`, which invokes the ValueTask awaiter's boxed async state-machine `MoveNext`.

Thus, with default channel options, a writer wakes N waiters and creates N global ThreadPool continuation work items even though only one item may be consumed by each continuation. This is a concrete mechanism, not an inference.

## Comparison and conclusion

**Measured fact:** C rises smoothly with waiter population, from approximately 0.5 µs at one waiter to 135–156 µs p50 and 206–234 µs p95 at 512 waiters. It does not show the production 150–200 **millisecond** distribution. The 512-waiter synthetic p99 is 217–264 µs, about three orders of magnitude lower.

**Inference — strongly supported:** Channel's all-waiter wake-up plus its mandatory global ThreadPool continuation queue explains the monotonic microsecond-scale population effect and thread hops. It can contribute to production interval C, but cannot by itself explain the measured millisecond-scale production tail on this host.

**Conclusion:** A) ThreadPool continuation dispatch: **STRONGLY SUPPORTED as a component**, **NOT SUPPORTED as the complete 150–200 ms explanation**. B) Channel waiter wake-up: **CONFIRMED** as the fan-out mechanism. C) Channel/scheduler interaction: **CONFIRMED** as the synthetic mechanism. D) workload/application behavior outside Channel: **STRONGLY SUPPORTED as required for the production-scale tail**. E) batching/eligibility: **REFUTED by the prior B interval and excluded from this benchmark**. F) another runtime mechanism: **NOT SUPPORTED by this experiment**.

## WHAT WE SHOULD CHANGE NEXT

Do not change production yet. First correlate production C episodes with EventPipe ThreadPool queue/dequeue and worker-adjustment events, and separately measure application work queued ahead of channel continuations. Repeat this exact benchmark on the production OS/runtime image before attributing any host-specific tail. Only after that evidence should a production topology change be considered.

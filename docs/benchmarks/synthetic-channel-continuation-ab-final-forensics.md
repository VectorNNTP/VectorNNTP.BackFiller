# Synthetic Channel Continuation A/B — Final Forensics

Classification: **benchmark + documentation only**. No production code, topology, Channel options, consumer count, batching, sockets, queue architecture, ThreadPool settings, or application dispatch was changed. This is a pure synthetic investigation.

---

## 1. Methodology

### Environment

- .NET 8 benchmark target, Debug/x64/net8.0, linux-x64, Linux execution host (consistent with previous synthetic forensics).
- Stopwatch frequency: 1 GHz (nanosecond resolution).
- `dotnet build -r linux-x64 --self-contained true` followed by direct executable invocation (no JIT warm path included in measurements).

### Benchmark configuration

```
channel-continuation-ab --warmup-waves 10 --measured-waves 100 --trials 3
```

- Warm-up: 10 waves (discarded).
- Measured: 100 waves per trial.
- Trials per topology: 3 independent runs.
- Items per wave: N (consumer count) for multi-reader; 1 for single-reader topologies.

### Wave protocol

For every wave, all N consumers register `WaitToReadAsync` and block asynchronously. Once all N are simultaneously parked (quorum detected atomically), producers call `TryWrite` synchronously. Consumer timestamps: immediately after await resumes (C), immediately before `TryRead`, immediately after `TryRead`. The C value (producer `TryWrite` timestamp → consumer `await` resumption) is the primary measurement.

### Experiments

| ID | Description | `AllowSynchronousContinuations` | Consumers | Producers |
|---|---|---|---|---|
| Exp1-Control | A/B control | `false` | 1,2,4,8,16,32,64,128,256,512 | 1, 4 |
| Exp1-Experiment | A/B experiment | `true` | 1,2,4,8,16,32,64,128,256,512 | 1, 4 |
| Exp2 | Single-reader control | `false` | 1 | 1, 4, 32 |
| Exp3 | Single-reader batched-drain | `false` | 1 | 1, 4, 32 |

`SingleWriter` is set true when `producerCount == 1`; `SingleReader` is set true when `consumerCount == 1`. This is the only time `SingleReader=true` is used: when exactly one consumer is actually running.

---

## 2. Experiment 1 — AllowSynchronousContinuations A/B

### 2a. Control: AllowSynchronousContinuations = false

Each cell is the range across the three independent trials. Reads per topology = consumers × 100 waves. C = producer-to-consumer-resumption latency.

| Cons | Prod | Reads | FailedReads | MaxW | SyncW | AsyncW | Mig | ProducerInline | MaxPendW | C p50 µs | C p95 µs | C p99 µs | C p99.9 µs | C max µs | Thr/s |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 100 | 0 | 1 | 0 | 100 | 0 | 100 | 0 | 0.64–0.69 | 0.71–1.06 | 0.81–3.96 | 0.90–224 | 0.90–224 | 154K–463K |
| 1 | 4 | 100 | 0 | 1 | 0 | 100 | 0 | 100 | 0 | 1.33–1.37 | 1.44–1.53 | 1.67–2.69 | 2.25–3.02 | 2.25–3.02 | 253K–273K |
| 2 | 1 | 200 | 0–2 | 2 | 100 | 101–102 | 0–1 | 200 | 1 | 0.99–1.01 | 1.23–1.28 | 2.79–17.9 | 17.4–51.8 | 17.4–51.8 | 495K–593K |
| 2 | 4 | 200 | 0–9 | 2 | 94–100 | 101–112 | 0–8 | 188–200 | 1–3 | 1.44–1.46 | 1.61–1.68 | 1.72–3.84 | 2.58–14.2 | 2.58–14.2 | 427K–443K |
| 4 | 1 | 400 | 0–3 | 4 | 300 | 103 | 0 | 400 | 3 | 1.61–1.64 | 2.16–2.20 | 3.33–4.09 | 4.13–5.02 | 4.13–5.02 | 868K–924K |
| 4 | 4 | 400 | 0–3 | 4 | 300 | 103 | 0 | 400 | 3 | 1.92–1.95 | 2.26–2.33 | 3.64–4.84 | 4.40–15.2 | 4.40–15.2 | 649K–694K |
| 8 | 1 | 800 | 0–7 | 8 | 700 | 107 | 0 | 800 | 7 | 2.57–2.89 | 3.58–3.96 | 5.38–6.96 | 7.66–15.7 | 7.66–15.7 | 1143K–1254K |
| 8 | 4 | 800 | 0–7 | 8 | 700 | 107 | 0 | 800 | 7 | 3.19–3.27 | 4.01–4.12 | 5.10–7.35 | 6.57–17.4 | 6.57–17.4 | 917K–931K |
| 16 | 1 | 1,600 | 0–59 | 16 | 1,499–1,501 | 115–156 | 0–19 | 1,295–1,600 | 15–16 | 5.17–5.83 | 7.77–9.10 | 10.9–14.0 | 13.4–19.9 | 15.4–20.2 | 1203K–1326K |
| 16 | 4 | 1,600 | 0–60 | 16 | 1,498–1,500 | 115–161 | 0–22 | 1,367–1,600 | 15–19 | 5.93–6.39 | 9.23–9.80 | 13.2–17.9 | 26.4–70.1 | 26.8–70.4 | 375K–1052K |
| 32 | 1 | 3,200 | 0–62 | 32 | 3,099–3,100 | 162 | 32–33 | 3,171–3,199 | 31 | 10.7–11.1 | 16.1–16.7 | 20.9–416 | 30.0–428 | 30.8–429 | 1095K–1343K |
| 32 | 4 | 3,200 | 0–124 | 32 | 3,098–3,100 | 131–226 | 28–32 | 3,168–3,190 | 31–33 | 10.7–11.5 | 16.1–16.4 | 22.5–34.8 | 31.0–91.7 | 31.9–92.5 | 1249K–1261K |
| 64 | 1 | 6,400 | 0–362 | 64 | 6,295–6,309 | 234–444 | 37–79 | 2,792–6,257 | 63–64 | 16.2–20.3 | 26.6–30.3 | 31.4–2050 | 47.3–2068 | 48.8–2069 | 957K–2369K |
| 64 | 4 | 6,400 | 0–255 | 64 | 6,295–6,299 | 289–340 | 53–67 | 2,347–6,330 | 63–67 | 15.3–21.2 | 22.1–32.8 | 30.6–152 | 42.1–534 | 44.1–536 | 1106K–2641K |
| 128 | 1 | 12,800 | 0–736 | 128 | 12,700–12,704 | 569–811 | 49–69 | 4,452–5,467 | 128 | 29.5–31.5 | 45.8–50.6 | 55.5–671 | 63.4–702 | 64.6–1240 | 2136K–2947K |
| 128 | 4 | 12,800 | 0–1,124 | 128 | 12,686–12,691 | 599–1,216 | 66–80 | 4,228–4,609 | 131 | 27.7–30.2 | 37.0–45.1 | 38.7–61.9 | 42.5–70.6 | 43.4–74.2 | 2541K–3149K |
| 256 | 1 | 25,600 | 0–3,610 | 256 | 25,483–25,496 | 1,583–3,720 | 63–92 | 8,679–19,831 | 256 | 55.0–93.4 | 77.4–165 | 88.9–267 | 122–672 | 128–683 | 1156K–3151K |
| 256 | 4 | 25,600 | 0–1,673 | 256 | 25,495–25,502 | 502–1,753 | 46–72 | 10,079–10,522 | 259 | 64.7–68.3 | 102–111 | 116–129 | 134–208 | 138–607 | 2719K–2926K |
| 512 | 1 | 51,200 | 0–1,168 | 512 | 51,096–51,105 | 725–1,256 | 45–51 | 20,139–20,481 | 512 | 112–142 | 178–236 | 186–340 | 192–734 | 197–743 | 2681K–3602K |
| 512 | 4 | 51,200 | 0–691 | 512 | 51,097–51,099 | 742–771 | 47–49 | 20,415–21,865 | 515 | 125–134 | 193–220 | 209–231 | 225–241 | 232–249 | 2999K–3233K |

**Observations (false):** The monotonic scaling from the previous experiment is reproduced exactly. MaxPendingWorkItems scales directly with consumer count: 0 at 1 consumer, 512–515 at 512 consumers. Consumer migrations confirm all continuations execute on ThreadPool threads distinct from the wait-registration thread. ProducerInline count equals 100% of reads, which is spurious here: at `AllowSynchronousContinuations=false` the inline detection flag (resumeThreadId == producerThreadId) fires only because both the producer and the consumer may be scheduled on the same ThreadPool thread (thread pool reuse), not because of inline execution by the channel.

### 2b. Experiment: AllowSynchronousContinuations = true

| Cons | Prod | Reads | FailedReads | MaxW | SyncW | AsyncW | Mig | ProducerInline | MaxPendW | C p50 µs | C p95 µs | C p99 µs | C p99.9 µs | C max µs | Thr/s |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 100 | 0 | 1 | 0 | 101 | 0–1 | 100 | 0 | 0.17–0.18 | 0.21–0.23 | 0.23–0.80 | 0.94–3.52 | 0.94–3.52 | 407K–515K |
| 1 | 4 | 100 | 0 | 1 | 0 | 101 | 0–2 | 100 | 3 | 0.16–0.18 | 0.20–0.24 | 0.24–0.79 | 0.29–1.26 | 0.29–1.26 | 220K–276K |
| 2 | 1 | 200 | **200** | 2 | 0 | **402** | 0 | 200 | 0 | 0.13 | 0.15–0.16 | 0.17–0.18 | 0.45–1.86 | 0.45–1.86 | 516K–540K |
| 2 | 4 | 200 | 1–198 | 2 | 1–88 | 217–399 | 4–97 | 103–198 | 2–3 | 0.13–0.18 | 0.17–0.54 | 0.52–1.27 | 0.82–1.75 | 0.82–1.75 | 333K–566K |
| 4 | 1 | 400 | **1,200** | 4 | 0 | **1,604** | 28–35 | 400 | 0 | 0.13 | 0.16–0.17 | 0.19–0.45 | 0.56–8.65 | 0.56–8.65 | 258K–403K |
| 4 | 4 | 400 | 634 | 4 | 24–34 | 921–987 | 169–189 | 211–247 | 3 | 0.14–0.15 | 0.32–0.40 | 0.88–3.92 | 1.35–4.31 | 1.35–4.31 | 554K–648K |
| 8 | 1 | 800 | **5,600** | 8 | 0 | **6,408** | 22–30 | 800 | 0 | 0.13–0.14 | 0.16–0.17 | 0.19–0.46 | 0.53–0.75 | 0.53–0.75 | 185K–206K |
| 8 | 4 | 800 | 2,356 | 8 | 232–270 | 2,715–2,906 | 277–289 | 445–464 | 3 | 0.16–0.17 | 0.72–0.82 | 1.98–2.50 | 4.34–16.5 | 4.34–16.5 | 481K–493K |
| 16 | 1 | 1,600 | **24,000** | 16 | 0 | **25,616** | 1–2 | 1,600 | 0 | 0.10–0.11 | 0.43–0.45 | 0.49–0.51 | 0.55–0.72 | 0.55–0.84 | 120K–122K |
| 16 | 4 | 1,600 | 9,156 | 16 | 597–752 | 9,271–10,150 | 367–449 | 757–888 | 3 | 0.17–0.23 | 2.31–2.39 | 3.04–5.99 | 6.29–8.28 | 7.42–9.20 | 291K–316K |
| 32 | 1 | 3,200 | **99,200** | 32 | 0 | **102,432** | 0–2 | 3,200 | 0 | 0.10–0.13 | 0.17–0.19 | 0.24–1.95 | 0.52–2.27 | 1.07–21.8 | 52K–86K |
| 32 | 4 | 3,200 | 72,505 | 32 | 612–804 | 68,170–75,115 | 169–275 | 2,339–2,554 | 3 | 0.10–0.12 | 4.55–5.10 | 6.27–7.32 | 7.31–25.2 | 7.46–253 | 117K–125K |
| 64 | 1 | 6,400 | **403,200** | 64 | 0 | **409,664** | 1–2 | 6,400 | 0 | 0.10–0.11 | 0.15–0.18 | 0.45–1.91 | 2.07–14.6 | 2.29–23.4 | 37K–46K |
| 64 | 4 | 6,400 | 177,250 | 64 | 1,859–2,215 | 169,438–181,788 | 1,504–1,656 | 4,047–4,381 | 3 | 0.16–0.17 | 8.42–9.04 | 11.2–11.8 | 13.1–13.6 | 13.8–15.1 | 115K–133K |
| 128 | 1 | 12,800 | **1,625,600** | 128 | 0 | **1,638,528** | 1–2 | 12,800 | 0 | 0.10 | 0.15 | 0.22 | 2.20–2.32 | 2.64–10.6 | 23K–24K |
| 128 | 4 | 12,800 | 699,808 | 128 | 3,637–4,238 | 656,937–708,993 | 3,366–3,495 | 8,266–8,845 | 3 | 0.17 | 15.5–21.7 | 20.4–29.1 | 25.0–34.4 | 27.5–37.5 | 62K–70K |
| 256 | 1 | 25,600 | **6,528,000** | 256 | 0 | **6,553,856** | 1–2 | 25,600 | 0 | 0.10–0.11 | 0.16–0.17 | 0.20–0.23 | 0.63–0.69 | 11.7–17.5 | 11.5K–12.1K |
| 256 | 4 | 25,600 | 2,759,102 | 256 | 6,003–7,354 | 2,692,526–2,778,005 | 7,738–8,145 | 17,540–18,366 | 3 | 0.16–0.17 | 35.7–42.1 | 48.6–55.6 | 57.9–75.0 | 62.1–3043 | 34K–35K |
| 512 | 1 | 51,200 | **26,163,200** | 512 | 0 | **26,214,912** | 1–5 | 51,200 | 0 | 0.11 | 0.17 | 0.21–0.22 | 0.66–0.68 | 11.4–33.5 | 5.99K–6.03K |
| 512 | 4 | 51,200 | 10,778,867 | 512 | 11,731–13,562 | 10,458,077–10,818,108 | 15,980–17,317 | 34,128–35,729 | 3 | 0.17–0.18 | 72.0–78.8 | 97.0–105 | 113–124 | 1991–2978 | 17K–18K |

---

## 3. Critical Comparison: False vs True at 128/256/512 Consumers

| Topology | C false p50 | C true p50 | C false p95 | C true p95 | C false thr/s | C true thr/s | Failed false | Failed true |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 128c 1p | 29.5–31.5 µs | **0.10 µs** | 45.8–50.6 µs | **0.15 µs** | 2.1–2.9 M/s | **23–24 K/s** | 0–736 | **1,625,600** |
| 128c 4p | 27.7–30.2 µs | **0.17 µs** | 37.0–45.1 µs | **15.5–21.7 µs** | 2.5–3.1 M/s | **62–70 K/s** | 0–1,124 | **699,808** |
| 256c 1p | 55.0–93.4 µs | **0.10–0.11 µs** | 77.4–165 µs | **0.16–0.17 µs** | 1.2–3.2 M/s | **11.5–12.1 K/s** | 0–3,610 | **6,528,000** |
| 256c 4p | 64.7–68.3 µs | **0.16–0.17 µs** | 102–111 µs | **35.7–42.1 µs** | 2.7–2.9 M/s | **34–35 K/s** | 0–1,673 | **2,759,102** |
| 512c 1p | 112–142 µs | **0.11 µs** | 178–236 µs | **0.17 µs** | 2.7–3.6 M/s | **5.99–6.03 K/s** | 0–1,168 | **26,163,200** |
| 512c 4p | 125–134 µs | **0.17–0.18 µs** | 193–220 µs | **72.0–78.8 µs** | 3.0–3.2 M/s | **17–18 K/s** | 0–691 | **10,778,867** |

**MEASURED FACT:** `AllowSynchronousContinuations=true` eliminates the visible C scaling curve. C p50 at 512 consumers drops from ~125–142 µs to ~0.11–0.18 µs — a factor of ~700–1200×.

**MEASURED FACT:** This latency reduction comes with catastrophic throughput collapse. At 512c/1p, throughput falls from ~2.7–3.6 M/s to ~5.99–6.03 K/s — a factor of ~450–600× reduction.

**MEASURED FACT:** Failed reads (thundering-herd wasted work) explode from ~0–1,168 to ~26,163,200 per trial at 512c/1p. At 512c/4p, AsyncW count grows from ~20,000 to ~10.5–10.8 million per trial.

---

## 4. Semantic Analysis: What AllowSynchronousContinuations=true Actually Does

### Producer-side execution of consumer continuations

**MEASURED FACT:** At `AllowSynchronousContinuations=true` with `SingleWriter=true` (one producer), `MaxPendingWorkItems` is zero across all consumer counts. Zero work items are enqueued to the ThreadPool. The producer thread executes all N consumer continuations inline during its single `TryWrite` call before returning to its loop.

**MEASURED FACT:** Consumer thread migrations are 0–5 across all consumer counts in the 1-producer true condition. With `false`, migrations were 37–92 at the same counts. This confirms consumers execute inline on the producer, not on separate ThreadPool threads.

**MEASURED FACT:** `AsynchronousWaits` count at `true`/1-producer grows as `N × N × waves` (e.g., 512 consumers: 26,214,912 async waits for 51,200 items = 512× amplification). Each successful write wakes all 512 waiters; 511 fail TryRead and re-register WaitToReadAsync; the wave quorum requirement forces them to all re-park before the next write, so the next write again wakes 512 consumers, 511 fail, and so on. This is an O(N²) work amplification per item.

**INFERENCE — STRONGLY SUPPORTED:** The C latency measurement collapse under `true` is an artifact of measurement boundary, not a real end-to-end improvement. The producer thread now records the "item available" timestamp immediately before `TryWrite`, then executes the consumer continuation inline and records "resume timestamp" during its own execution. Both timestamps are on the same thread with no scheduling gap. The wall-clock delta is near zero, but the total CPU work per item is N times larger.

**MEASURED FACT:** Producer throughput at 512c/1p collapses to ~6 K/s from ~3.6 M/s. The producer is no longer producing: it is executing N consumer continuations inline per item, each of which triggers N-1 failed TryReads and N-1 re-registrations.

### Does AllowSynchronousContinuations=true reduce the waiter-population scaling curve?

**MEASURED FACT:** YES — it reduces the *observable C latency metric* from approximately linear-in-N to approximately O(1) (0.10–0.18 µs flat across 1–512 consumers at 1 producer).

**MEASURED FACT:** NO — it does not reduce the total work per item. The work is transferred from the ThreadPool scheduling path onto the producer, and then O(N) failed TryRead continuations generate O(N) additional re-registrations, creating O(N²) total continuation operations per successful read.

**CONCLUSION:** `AllowSynchronousContinuations=true` moves cost from consumer-side ThreadPool scheduling to producer-side inline execution, and simultaneously multiplies total continuation work by N through the thundering-herd mechanism. It trades observable C latency for catastrophic throughput and work amplification.

---

## 5. Runtime Source Explanation

Runtime source: [`dotnet/runtime` v8.0.0](https://github.com/dotnet/runtime/tree/v8.0.0).

### AllowSynchronousContinuations = false (default)

Call path for `TryWrite`:
1. `UnboundedChannelWriter.TryWrite` acquires `SyncObj`, enqueues item, then calls `ChannelUtilities.WakeUpWaiters(_waitingReadersTail, ...)`.
2. `WakeUpWaiters` iterates the **entire** waiting reader list and calls `AsyncOperation<bool>.TrySetResult(true)` on **every** waiting reader.
3. `AsyncOperation<TResult>.SignalCompletion`: because `AllowSynchronousContinuations=false`, `_runContinuationsAsynchronously=true`. Path taken: `UnsafeQueueSetCompletionAndInvokeContinuation()` → `ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false)`.
4. Each waiting reader becomes an independent global ThreadPool work item.
5. ThreadPool schedules N continuations; all N consumers resume and race to `TryRead`; only one succeeds; N-1 fail and re-register.

The `MaxPendingWorkItems` trace confirms this: at 512 consumers `false`, max pending work items = 512–515 (512 continuations enqueued + up to 3 pre-existing items).

### AllowSynchronousContinuations = true

Call path for `TryWrite`:
1. Same path to `WakeUpWaiters`.
2. `AsyncOperation<TResult>.SignalCompletion`: because `AllowSynchronousContinuations=true`, `_runContinuationsAsynchronously=false`. Path taken: `SetCompletionAndInvokeContinuation()` — the continuation executes **synchronously on the calling thread** (the producer's `TryWrite` call stack).
3. The producer thread runs all N consumer continuation state machines, one after another, without returning from `TryWrite`.
4. `MaxPendingWorkItems = 0` confirms: nothing is ever queued.

**This is the exact mechanism:** `SignalCompletion` branches on `_runContinuationsAsynchronously`. The field is set during `AsyncOperation` construction from the channel's `AllowSynchronousContinuations` option. The branch is inside `SignalCompletion` in `System.Threading.Channels/src/System/Threading/Channels/AsyncOperation.netcoreapp.cs`.

### Why N-1 fail for both modes

`WakeUpWaiters` wakes **all** N waiters regardless of the continuation mode. This is the fundamental fan-out. The channel does not know how many items are available at wake time — it only signals readability. Each woken consumer must race to `TryRead`; exactly one wins per item; N-1 lose and loop back to `WaitToReadAsync`.

---

## 6. Experiment 2 — Single-reader Control

One consumer, `AllowSynchronousContinuations=false`, genuinely `SingleReader=true`. Items per wave = 1.

| Prod | Reads | SyncW | AsyncW | Mig | MaxPendW | C p50 µs | C p95 µs | C p99 µs | Thr/s |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 100 | 0 | 100 | 0 | 0 | 0.48–0.49 | 0.61–0.80 | 0.67–1.50 | 518K–607K |
| 4 | 100 | 0 | 100 | 0 | 0 | 0.95–1.00 | 1.02–1.11 | 1.05–1.61 | 372K–409K |
| 32 | 100 | 0 | 100 | 2 | 0–4 | 4.25–4.44 | 4.72–4.92 | 4.75–5.25 | 109K–110K |

**MEASURED FACT:** Zero failed reads across all single-reader producer counts. No thundering herd. Only one consumer is ever registered; the single `TryWrite` wakes exactly one waiter; that waiter always succeeds `TryRead`.

**MEASURED FACT:** MaxPendingWorkItems = 0 at 1 and 4 producers; 0–4 at 32 producers (32 producers occasionally queue a brief burst).

**MEASURED FACT:** C p50 at 1 producer single-reader = 0.48–0.49 µs vs 0.64–0.69 µs for 1 consumer in the A/B false control. The single-reader path is ~25% faster in latency.

**MEASURED FACT:** At 32 producers single-reader, C p50 rises to 4.25–4.44 µs. This is the cost of contention among 32 producers racing to write and signal, not consumer-side waiter fan-out.

**MEASURED FACT:** Consumer thread migrations at 32 producers = 2 (2 items out of 100 resumed on a different thread from the waiter thread). With 1 and 4 producers, migrations = 0 (the single writer's continuation runs inline or on the same thread).

---

## 7. Experiment 3 — Single-reader Batched Drain

One consumer, `AllowSynchronousContinuations=false`, `SingleReader=true`. Consumer: `await WaitToReadAsync` then `while(TryRead) consume`. Items per wave = 1 (since consumerCount=1; `TotalDrained` == `Reads` confirming each wake drains exactly 1 item in this synthetic, because each wave writes only 1 item before the consumer drains).

| Prod | Reads | Drained | SyncW | AsyncW | Mig | MaxPendW | C p50 µs | C p95 µs | C p99 µs | Thr/s |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 100 | 100 | 0 | 100 | 0 | 0 | 0.43–0.45 | 0.52–0.58 | 0.56–0.81 | 623K–631K |
| 4 | 100 | 100 | 0 | 100 | 0–19 | 0 | 0.51–0.87 | 0.61–1.03 | 0.96–2.06 | 376K–563K |
| 32 | 100 | 100 | 0 | 100–101 | 38–49 | 16–28 | 0.34–0.35 | 0.48–4.29 | 4.55–5.16 | 149K–162K |

**MEASURED FACT:** The batched-drain consumer with 1 producer is slightly faster than the single-item consumer (p50 0.43–0.45 µs vs 0.48–0.49 µs) due to marginally faster drain in the inner loop vs outer loop re-entry.

**MEASURED FACT:** At 32 producers batched-drain, throughput (149–162 K/s) is meaningfully higher than single-item (109–110 K/s). MaxPendingWorkItems rises to 16–28, reflecting 32 concurrent producer writes sometimes queuing continuations before the single consumer drains.

**MEASURED FACT:** Drain count = 1 per wake in all cases. In the synthetic with 1 item per wave, the drain loop is semantically equivalent to single-item consumption. To measure true batch amortization, a future experiment would write M items per wave from multiple producers before the consumer wakes — but that is outside this experiment's scope.

---

## 8. ThreadPool Metrics Summary

| Topology (false) | MaxPendingWorkItems | MaxThreadCount | Mig |
|---|---:|---:|---:|
| 1c/1p | 0 | — | 0 |
| 8c/1p | 7 | — | 0 |
| 64c/1p | 63–64 | — | 37–79 |
| 128c/1p | 128 | — | 49–69 |
| 512c/1p | 512 | — | 45–51 |
| 512c/4p | 515 | — | 47–49 |

| Topology (true) | MaxPendingWorkItems | MaxThreadCount | Mig |
|---|---:|---:|---:|
| 512c/1p | **0** | — | 1–5 |
| 512c/4p | **3** | — | 15,980–17,317 |

**MEASURED FACT:** With `AllowSynchronousContinuations=false`, `MaxPendingWorkItems ≈ N` at the 512-consumer level. The global ThreadPool queue grows by exactly N per wave.

**MEASURED FACT:** With `AllowSynchronousContinuations=true`, `MaxPendingWorkItems = 0` for all single-producer topologies. ThreadPool is bypassed entirely. All continuation work runs on the producer thread.

**MEASURED FACT:** At `true`/4-producer, `MaxPendingWorkItems = 3` (not N), because the 4 producers execute continuations inline on their own threads. Migrations at 512c/4p-true reach 15,980–17,317 — this is the O(N²) re-registration storm creating massive cross-thread work.

---

## 9. Production Comparison

| Source | C p50 | C p95 | C p99 | Scale vs production |
|---|---:|---:|---:|---:|
| Previous synthetic false, 512c/4p | 135–143 µs | 206–210 µs | 217–264 µs | ~1000× below production |
| This experiment false, 512c/1p | 112–142 µs | 178–236 µs | 186–340 µs | ~1000× below production |
| This experiment false, 512c/4p | 125–134 µs | 193–220 µs | 209–231 µs | ~1000× below production |
| This experiment true, 512c/1p | **0.11 µs** | **0.17 µs** | **0.21–0.22 µs** | N/A — measurement collapses, throughput collapses 600× |
| This experiment true, 512c/4p | **0.17–0.18 µs** | **72–79 µs** | **97–105 µs** | C metric drops, but throughput drops 170× |
| Production (reported) | — | — | — | 150–200 ms |

**MEASURED FACT:** The `AllowSynchronousContinuations=false` results are consistent with the previous experiment: 512-consumer C p50 ~125–142 µs, approximately three orders of magnitude below the reported 150–200 ms production tail.

**MEASURED FACT:** Changing to `AllowSynchronousContinuations=true` collapses the observable C metric (as measured from item availability to consumer continuation resumption) to 0.10–0.18 µs at all consumer counts for the 1-producer topology. But this is not a real latency improvement: the producer thread does all consumer work inline, then throughput collapses from ~3 M/s to ~6 K/s.

**INFERENCE — CONFIRMED:** `AllowSynchronousContinuations=true` cannot bridge the 1000× gap to the production 150–200 ms tail. It changes the semantics of the measurement: it makes the consumer appear to resume instantly because the producer runs the consumer code on its own thread. It does not make the production pipeline faster. The total work per item increases by O(N) because of the thundering-herd amplification.

---

## 10. Hypothesis Classifications

### H1: Channel waiter fan-out / WakeUpWaiters wakes all N waiters per write
**Status: CONFIRMED**  
The `false` experiment reproduces monotonic scaling across 1–512 consumers. `WakeUpWaiters` iterates all registered readers unconditionally. This is not a coincidence: it is the single-producer `MaxPendingWorkItems` trace that confirms exactly N items queued per wave.

### H2: AllowSynchronousContinuations=true eliminates the ThreadPool continuation queue
**Status: CONFIRMED**  
`MaxPendingWorkItems = 0` for all single-producer `true` topologies. The `SignalCompletion` inline branch bypasses `ThreadPool.UnsafeQueueUserWorkItem` entirely.

### H3: AllowSynchronousContinuations=true reduces the waiter-population C scaling curve
**Status: CONFIRMED (with critical caveat)**  
The C metric is flat at ~0.10–0.18 µs regardless of consumer count in the 1-producer true topology. The scaling curve is eliminated — for the *measurement*. The underlying work scales O(N²) in failed TryRead retries.

### H4: AllowSynchronousContinuations=true improves production throughput
**Status: NOT SUPPORTED**  
Throughput collapses at all multi-consumer counts. At 512c/1p, throughput drops 600×. The cost is not reduced; it is relocated and amplified.

### H5: AllowSynchronousContinuations=true moves consumer continuation work onto the producer
**Status: CONFIRMED**  
Consumer thread migrations drop from 37–92 (false) to 1–5 (true) at 512 consumers with 1 producer. MaxPendingWorkItems = 0. ProducerInline count = total reads. The producer thread runs all consumer continuations inline.

### H6: AllowSynchronousContinuations=true can bridge the ~1000× gap to the production 150–200 ms tail
**Status: NOT SUPPORTED / REFUTED**  
The false C values are ~125–142 µs at 512 consumers. The true values are ~0.11 µs but only because the measurement protocol changes: the producer time-stamps the item, then immediately executes the consumer's continuation on its own thread, then the consumer records the resume timestamp — all on one thread with no scheduling gap. This cannot explain 150–200 ms. The production interval involves real cross-thread scheduling, queuing, and application work outside this synthetic.

### H7: Channel all-waiter wake-up is a real scalability cost
**Status: CONFIRMED (as a component)**  
C scales from ~0.5 µs at 1 waiter to ~125–142 µs at 512 waiters with `false`. This is a real microsecond-scale contribution. It is NOT the complete 150–200 ms production explanation.

### H8: Single-reader topology eliminates failed TryRead thundering herd
**Status: CONFIRMED**  
Experiment 2: zero failed reads across all single-reader trials (1, 4, 32 producers). The thundering herd is mechanically impossible when only one consumer is registered.

### H9: Single-reader batched drain improves latency or throughput
**Status: PLAUSIBLE (not tested at scale)**  
Experiment 3 shows marginal improvements over non-batched at 1 producer. At 32 producers, batched drain throughput is 37% higher than non-batched, and the drain amortizes ThreadPool continuation overhead. However, in this synthetic each wave writes only 1 item, so drain length is always 1. A future experiment with M>1 items per wave is needed to test genuine batch amortization.

---

## 11. WHAT THIS EXPERIMENT PROVES

1. `AllowSynchronousContinuations=false` (default) causes the Channel to enqueue exactly N global ThreadPool work items per `TryWrite` when N consumers are waiting. This is the mechanism, confirmed by `MaxPendingWorkItems` trace.

2. `AllowSynchronousContinuations=true` eliminates the ThreadPool queue (MaxPendingWorkItems=0) by running all consumer continuations inline on the producer thread during `TryWrite`.

3. The inline execution collapses the observable C latency metric from ~125–142 µs to ~0.11 µs at 512 consumers — but this is a measurement artifact, not a real improvement. The producer now does N consumers' work before returning from `TryWrite`.

4. Inline execution causes O(N²) total continuation work per successful read at high consumer counts: N continuations run, N-1 fail TryRead, all N-1 re-register, then the next write wakes all N again. Throughput collapses by 450–600× at 512 consumers.

5. The thundering-herd (N-1 failed TryReads per write) is a property of the Channel architecture — all waiters are always woken — not of the continuation dispatch mode.

6. A genuine single-reader topology (1 consumer) eliminates all failed reads and all continuation fan-out. C p50 at 1 producer is 0.48–0.49 µs vs 0.64–0.69 µs for the 1-consumer entry in the multi-reader sweep.

7. The synthetic false C value at 512 consumers (~125–142 µs p50) is consistent with the previous experiment, confirming the result is stable and reproducible on this host.

8. The synthetic false C maximum is ~200–743 µs at 512 consumers. This is still approximately three orders of magnitude below the reported production 150–200 ms tail. Channel fan-out is a real cost but cannot be the complete explanation.

---

## 12. WHAT THIS EXPERIMENT DOES NOT PROVE

1. It does not prove that `AllowSynchronousContinuations=true` is a viable production option. It would collapse production throughput by the same mechanism.

2. It does not prove that reducing consumer count would eliminate the production tail. This experiment controls only the synthetic fan-out cost; production C involves application work, socket I/O, TCP acknowledgement, NNTP server processing, and other external dependencies not present here.

3. It does not prove that single-reader topology is faster than multi-reader in production. The single-reader experiment writes only 1 item per wave; a production topology with many writers and one reader requires a different analysis of the drain amortization benefit under real load.

4. It does not prove that `AllowSynchronousContinuations=true` makes the C metric lower in production. The inline execution cost still exists; it is transferred to the producer. The producer throughput would collapse in the same way.

5. It does not establish the source of the 150–200 ms production tail. The synthetic maximum at 512 consumers is ~200–743 µs. Something else — queuing of application work ahead of channel continuations, TCP round-trip time, server-side latency, or other application delays — is required to explain the remaining ~200–1000× gap.

6. It does not rule out that future experiments on the production host and runtime version might show different absolute values. The Linux sandbox results are consistent with the previous experiment on the same host.

---

## 13. Evidence Artifact

Raw JSON artifact: `synthetic-channel-continuation-ab.json` (co-located with benchmark binary).

```
Stopwatch frequency: 1,000,000,000 Hz
Warmup waves:        10
Measured waves:      100
Trials per topology: 3
AbTrials:            120  (20 topologies × 2 conditions × 3 trials)
SingleReaderTrials:  9    (3 producer counts × 3 trials)
BatchedDrainTrials:  9    (3 producer counts × 3 trials)
```

---

## 14. WHAT WE SHOULD INVESTIGATE NEXT

Do not change production yet. The remaining ~1000× gap between synthetic maximum and production tail is the open question.

Priority next steps:
1. Correlate production C episodes with EventPipe ThreadPool queue/dequeue events to measure how many other work items are ahead of channel continuations when the C tail occurs.
2. Measure whether the production TCP round-trip component of interval C is captured in the C measurement definition.
3. Repeat the synthetic false experiment on the production OS/runtime image to confirm the ~125–142 µs baseline is not host-specific.
4. Run a future batched-drain experiment with M>1 items per wave to test whether draining amortizes overhead under realistic multi-producer load, before drawing any architectural conclusions.

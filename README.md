# VectorNNTP.BackFiller

**High-performance Usenet article backfilling and recovery service**

VectorNNTP.BackFiller is a high-performance .NET service responsible for retrieving Usenet articles that are unavailable from the primary VectorNNTP storage system.

Articles may require backfilling because they have:

- Expired from primary storage
- Been removed as a result of DMCA or other takedown requests
- Failed to be stored successfully
- Become corrupted or otherwise unusable
- Been administratively removed
- Become unavailable for another operational reason

BackFiller connects to one or more external Usenet providers, retrieves the requested articles, validates the retrieved data, and returns the recovered content to the VectorNNTP processing pipeline.

The service is designed for **very high throughput, high concurrency, predictable resource utilisation, and reliable graceful shutdown**.

---

## Overview

VectorNNTP.BackFiller forms part of the VectorNNTP article-recovery pipeline.

At a high level:

                    +----------------------+
                    |   VectorNNTP Core    |
                    |      Services        |
                    +----------+-----------+
                               |
                         Backfill Request
                               |
                               v
                    +----------------------+
                    | VectorNNTP.BackFiller|
                    |                      |
                    |  Request Processing  |
                    |  Work Management     |
                    |  Concurrency         |
                    |  Provider Selection  |
                    |  Retrieval           |
                    |  Validation          |
                    |  Recovery            |
                    +----------+-----------+
                               |
                 +-------------+-------------+
                 |             |             |
                 v             v             v
             Provider A    Provider B    Provider C
                 |             |             |
                 +-------------+-------------+
                               |
                               v
                       Recovered Article

BackFiller is deliberately designed so that external Usenet provider latency and failures do not unnecessarily propagate through the rest of the VectorNNTP system.

---

## Design Goals

### Performance

BackFiller is designed to operate at extremely high network throughput and sustain large numbers of concurrent article retrievals.

The architecture prioritises:

- Asynchronous I/O
- High connection concurrency
- Efficient buffering
- Low allocation rates
- Minimal unnecessary copying
- Efficient work scheduling
- Backpressure
- Predictable memory usage
- Efficient cancellation and shutdown
- Avoidance of unnecessary synchronisation

The target deployment environment is capable of handling **multi-10-Gbps Usenet traffic**, with the implementation designed to scale with available CPU, memory, network bandwidth, and upstream provider capacity.

### Reliability

External Usenet providers are inherently unreliable from the perspective of a distributed service.

BackFiller therefore treats the following as expected operational conditions:

- Connection failures
- Authentication failures
- Timeouts
- Provider errors
- Missing articles
- Incomplete responses
- Corrupt responses
- Connection resets
- Rate limiting
- Temporary provider failures

Where appropriate, requests can be retried or redirected to another configured provider.

### Concurrency

BackFiller is intended to maintain high levels of concurrent work without allowing concurrency to become uncontrolled resource consumption.

Work admission, active work accounting, provider connection limits, and shutdown behaviour are explicit parts of the service architecture.

### Graceful Shutdown

Shutdown is treated as a first-class operational requirement.

The service distinguishes between:

- Work that has been admitted
- Work that is queued but not yet admitted
- Work currently executing
- Work that has completed
- Work that has been cancelled

A critical invariant is that **every admitted unit of work must eventually be settled exactly once**, regardless of the termination path taken.

---

## Core Responsibilities

BackFiller is responsible for the following broad stages of the recovery process:

1. Receive a backfill request.
2. Validate the request.
3. Admit work according to configured concurrency and capacity limits.
4. Select an appropriate upstream provider.
5. Establish or reuse the required connection.
6. Request the required article.
7. Receive and process the response.
8. Validate the retrieved article.
9. Return the recovered article to the VectorNNTP pipeline.
10. Report success or failure.
11. Release all resources and accounting associated with the request.

Failures are handled according to the applicable retry, provider, cancellation, and recovery policies.

---

## Architecture

The service is composed of several logical areas.

### Request Processing

Responsible for accepting and validating backfill requests and placing work into the appropriate processing pipeline.

### Work Management

Controls admission, concurrency, queueing, cancellation, and completion accounting.

The work-management layer is particularly important because BackFiller can operate with very large numbers of simultaneous operations.

### Provider Management

Maintains the configured upstream Usenet providers and controls provider selection, connection management, limits, and failure handling.

### Retrieval

Performs the actual NNTP article retrieval and response processing.

### Validation

Validates retrieved data before it is considered successfully recovered.

### Settlement

Ensures that admitted work is correctly accounted for regardless of whether it completes successfully, fails, or is cancelled.

---

## Cancellation and Shutdown

BackFiller makes an explicit distinction between **cancelling work admission** and **cancelling already-admitted work**.

This distinction is important during graceful shutdown.

Conceptually:

                 Shutdown Requested
                         |
                         v
               Stop accepting work
                         |
                         v
              +-------------------+
              | Already admitted? |
              +---------+---------+
                        |
              +---------+---------+
              |                   |
             No                  Yes
              |                   |
              v                   v
        Discard/stop        Complete or cancel
        pending work        admitted work
                                  |
                                  v
                         Settle drain accounting
                                  |
                                  v
                           Shutdown complete

A critical invariant is that **every admitted unit of work must eventually be settled exactly once**, including cancellation paths where an admitted delivery does not result in an NNTP `ACK` or `NACK`.

---

## Performance Characteristics

BackFiller is intended for deployments where throughput and concurrency are significant operational requirements.

Performance-sensitive areas include:

- Network I/O
- NNTP protocol processing
- Buffer management
- Memory allocation
- Concurrent work scheduling
- Queue operations
- Connection management
- Provider selection
- Cancellation
- Completion accounting
- Logging and telemetry

Performance changes should therefore be evaluated not only for functional correctness but also for their impact on:

- Throughput
- Latency
- Tail latency
- CPU utilisation
- Allocation rate
- Garbage collection
- Memory consumption
- Lock contention
- Connection utilisation
- Queue depth

Micro-optimisations should not be introduced at the expense of correctness, observability, or maintainability without measurable benefit.

---

## Configuration

Configuration is environment-specific and should be supplied through the application's supported .NET configuration mechanisms.

Typical configuration areas include:

- Upstream Usenet providers
- Provider credentials
- Connection limits
- Concurrency limits
- Request timeouts
- Retry policies
- Queue capacity
- Logging
- Telemetry
- Operational limits

**Credentials and other secrets must never be committed to source control.**

For development, use appropriate local configuration mechanisms such as user secrets or environment variables.

For production, secrets should be supplied through the deployment environment's secure secret-management facilities.

---

## Building

Clone the repository:

    git clone <repository-url>
    cd VectorNNTP.BackFiller

Restore dependencies:

    dotnet restore

Build the solution:

    dotnet build --configuration Release

---

## Testing

Run the test suite with:

    dotnet test --configuration Release

For development, the normal workflow is:

    dotnet restore
    dotnet build
    dotnet test

Performance-sensitive changes should additionally be evaluated using the project's performance and load-testing infrastructure where applicable.

---

## Development Principles

Changes to BackFiller should preserve the following principles.

### Correctness Before Optimisation

A faster implementation that occasionally loses work, double-settles work, leaks resources, or mishandles cancellation is not an optimisation.

### Explicit Ownership

Resources and work ownership should be clear.

Every admitted operation should have an unambiguous completion and settlement path.

### Cancellation Must Be Deliberate

Cancellation behaviour should be explicitly considered whenever modifying:

- Queues
- Consumers
- Producers
- Work services
- Network operations
- Shutdown logic
- Drain accounting

### Avoid Unnecessary Allocations

The service is intended for sustained high-throughput operation.

Allocation-sensitive code should avoid unnecessary:

- Temporary objects
- Buffer copies
- String allocations
- LINQ in hot paths
- Boxing
- Delegate creation
- Closure allocations

where measurable performance benefits justify the additional complexity.

### Measure Before and After

Performance changes should be supported by measurements rather than assumptions.

Useful measurements include:

- Requests/sec
- Articles/sec
- Network throughput
- Mean latency
- P95 latency
- P99 latency
- CPU utilisation
- GC activity
- Allocation rate
- Active connections
- Queue depth
- Provider utilisation

---

## Logging and Observability

Operational diagnostics are important because many failures occur outside the service itself.

Logging should provide enough information to diagnose:

- Provider failures
- Authentication failures
- Connection failures
- Article-not-found responses
- Retries
- Timeouts
- Cancellation
- Queue saturation
- Backpressure
- Shutdown and drain behaviour
- Unexpected protocol responses

High-frequency hot paths should avoid excessive logging.

Where possible, high-volume operational information should be represented through metrics rather than individual log messages.

---

## Security

Security issues should **not** be reported through public GitHub issues.

Please follow the repository's security policy for reporting vulnerabilities.

In particular:

- Never commit credentials.
- Never commit provider passwords.
- Never commit API keys or access tokens.
- Do not include secrets in logs.
- Do not expose sensitive provider configuration through diagnostics.
- Treat external provider responses as untrusted input.

---

## Repository Structure

The repository is organised around the service runtime, infrastructure, and test requirements.

The exact project structure may evolve as the implementation develops.

    VectorNNTP.BackFiller/
    |
    +-- .github/
    |   +-- workflows/
    |   +-- dependabot.yml
    |   +-- CODEOWNERS
    |   +-- SECURITY.md
    |
    +-- src/
    |   +-- ...
    |
    +-- tests/
    |   +-- ...
    |
    +-- benchmarks/
    |   +-- ...
    |
    +-- global.json
    +-- Directory.Build.props
    +-- Directory.Packages.props
    +-- README.md

---

## CI/CD

Pull requests are expected to pass the repository's automated validation before being merged.

CI should validate, as applicable:

- Restore
- Compilation
- Unit tests
- Integration tests
- Dependency security
- Static analysis
- CodeQL analysis

Dependency updates are managed through Dependabot.

GitHub Actions dependencies are also monitored so that CI infrastructure itself remains current.

---

## Versioning and Releases

Release and deployment procedures are managed independently from normal development builds.

Production releases should be:

- Reproducible
- Traceable to a specific commit
- Built from validated source
- Clearly versioned
- Accompanied by appropriate release notes

---

## Contributing

Changes should generally be submitted through pull requests.

Before opening a pull request:

1. Build the solution.
2. Run the test suite.
3. Verify that existing functionality remains intact.
4. Add regression tests for behavioural changes.
5. Review concurrency and cancellation implications.
6. Review performance implications for hot-path changes.
7. Ensure no secrets or environment-specific configuration has been committed.

Keep pull requests focused. Avoid mixing unrelated refactoring with functional changes unless the refactoring is required for the change being made.

---

## License

Copyright ©2026 Chris Knipe <cknipe@opticnetworks.net>.

License information will be provided here when the project's licensing terms are finalised.

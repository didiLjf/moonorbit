# Changelog

All notable changes to the MoonOrbit project will be documented in this file.

## [0.2.1] - 2026-08-16

- Updated the project metadata and generated interface with the official MoonBit stable toolchain v0.10.7.
- Added cross-platform CI execution for all five CLI application scenarios.

## [0.2.0] - 2026-07-28

### Added
- **Timer & Scheduling**: Labeled optional scheduling functions `send_after` and `send_repeatedly` with cancellable `TimerSubscription`.
- **Stash/Unstash**: Actor context secondary stash buffer helper allowing buffering and restoring of unprocessable messages.
- **Finite State Machine (FSM)**: Complete FSM framework helper supporting state transitions, metadata, and state-level timeout timers.
- **PubSub Mediator**: Built-in generic Publish-Subscribe event stream manager actor for topic-based pub/sub patterns.
- **Cluster Simulation**: Virtual network simulation package supporting location-transparent `RemoteRef` routing, packet drops, and transmission latency.
- **New Showcase Examples**:
  - `cmd/distributed_kv`: Distributed master-replica consensus database.
  - `cmd/work_pulling`: Overload-resistant worker pull concurrency pattern.
  - `cmd/benchmarks`: CPU clock-accurate ring benchmark for throughput profiling.

### Changed
- Converted `options("is-main": true)` to `pkgtype(kind: "executable")` across all executable packages to conform to the latest MoonBit toolchain requirements.
- Regenerated type-checking interface definitions (`pkg.generated.mbti`).
- Reformatted all codebase files using `moon fmt`.
- Updated `README.md` to comprehensively document new components and execution targets.

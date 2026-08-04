# Changelog

All notable changes to CrowKV will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Copyright headers on all source files
- AGENTS.md with project overview and dispatch table
- CONTRIBUTING.md, PR/Issue templates
- Demo recording plan (`doc/working/plan-demo.md`)

### Changed
- Restructured agent workflows: conventions merged into `/coding` workflow
- Slimmed coding.md, doc.md, review.md
- README: added badges, folded Getting Started into `<details>`

## [0.1.0] - 2026-07-13

### Added
- Multi-Paxos consensus with per-key slot pipelining and out-of-order apply
- WAL with multi-disk segments, batched durable flush, replay, and GC
- crow-tree storage engine: B+tree with delta chains, io_uring async I/O, epoch-safe lock-free reads, buffer pool
- `KVEngine` trait with in-memory and crow-tree backends
- gRPC services: Paxos (Prepare/Promise/Accept/Accepted), KV, Snapshot
- Leader election with term/ballot fencing and leader lease
- Reconfiguration: member add/remove, leader transfer, membership epoch fence
- `crowkv-server` binary with HTTP management API
- `crowkv-client` library with topology cache, retry, idempotency
- `crowkv-console`: web UI (Axum + React) and CLI for cluster lifecycle management
- Comprehensive design documentation (`doc/`)
- CI with GitHub Actions (fmt, clippy, test, Playwright E2E)
- Pre-commit hooks (cargo fmt, clippy, clang-format, clang-tidy)

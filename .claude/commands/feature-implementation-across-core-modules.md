---
name: feature-implementation-across-core-modules
description: Workflow command scaffold for feature-implementation-across-core-modules in alchemy-solana-bot.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /feature-implementation-across-core-modules

Use this workflow when working on **feature-implementation-across-core-modules** in `alchemy-solana-bot`.

## Goal

Implements a new major feature (e.g., arbitrage executor, backrun engine) by updating or creating multiple core Rust modules, associated protobuf definitions, and build configuration.

## Common Files

- `src/executor.rs`
- `src/arb.rs`
- `src/jito.rs`
- `src/main.rs`
- `protos/searcher.proto`
- `protos/bundle.proto`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Update or create relevant .rs files in src/ (e.g., executor.rs, arb.rs, main.rs, jito.rs)
- Update or add protobuf definitions in protos/ (e.g., searcher.proto, bundle.proto, packet.proto, shared.proto)
- Update build configuration (Cargo.toml, Cargo.lock, build.rs) if dependencies or build steps change

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.
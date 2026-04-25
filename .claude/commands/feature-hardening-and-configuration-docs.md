---
name: feature-hardening-and-configuration-docs
description: Workflow command scaffold for feature-hardening-and-configuration-docs in alchemy-solana-bot.
allowed_tools: ["Bash", "Read", "Write", "Grep", "Glob"]
---

# /feature-hardening-and-configuration-docs

Use this workflow when working on **feature-hardening-and-configuration-docs** in `alchemy-solana-bot`.

## Goal

Hardens an existing feature by adding configuration options, environment variables, updating documentation, and providing usage instructions.

## Common Files

- `src/executor.rs`
- `src/arb.rs`
- `src/main.rs`
- `.env.example`
- `README.md`

## Suggested Sequence

1. Understand the current state and failure mode before editing.
2. Make the smallest coherent change that satisfies the workflow goal.
3. Run the most relevant verification for touched files.
4. Summarize what changed and what still needs review.

## Typical Commit Signals

- Update core logic files in src/ (e.g., executor.rs, arb.rs, main.rs) to support new config/env vars and improve code quality
- Add or update environment variable documentation in .env.example
- Update README.md with new instructions, setup steps, and warnings

## Notes

- Treat this as a scaffold, not a hard-coded script.
- Update the command if the workflow evolves materially.
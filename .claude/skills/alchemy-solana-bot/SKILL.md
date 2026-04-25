```markdown
# alchemy-solana-bot Development Patterns

> Auto-generated skill from repository analysis

## Overview
This skill covers the core development patterns and workflows for the `alchemy-solana-bot` Rust codebase. The repository implements advanced Solana trading bots, with modular architecture, protobuf-based communication, and robust configuration. You'll learn the project's coding conventions, how to implement new features across core modules, and how to harden features with configuration and documentation.

## Coding Conventions

- **File Naming:**  
  Use `camelCase` for Rust source files.  
  _Example:_  
  ```
  src/arbitrageExecutor.rs
  src/backrunEngine.rs
  ```

- **Import Style:**  
  Use **relative imports** within the crate.  
  _Example:_  
  ```rust
  mod arb;
  use crate::executor::Executor;
  use super::jito::JitoClient;
  ```

- **Export Style:**  
  Use **named exports** for modules and functions.  
  _Example:_  
  ```rust
  pub mod arb;
  pub fn run_executor() { ... }
  ```

- **Commit Messages:**  
  Follow [Conventional Commits](https://www.conventionalcommits.org/), using `feat` as the prefix for new features.  
  _Example:_  
  ```
  feat: add backrun engine to core modules
  ```

## Workflows

### Feature Implementation Across Core Modules
**Trigger:** When you want to add a new core feature or engine to the bot.  
**Command:** `/new-feature`

1. **Update or create relevant Rust files in `src/`**  
   - Add or modify modules such as `executor.rs`, `arb.rs`, `main.rs`, `jito.rs`.
   - _Example:_  
     ```rust
     // src/arb.rs
     pub struct ArbitrageEngine { /* fields */ }
     impl ArbitrageEngine {
         pub fn execute(&self) { /* logic */ }
     }
     ```

2. **Update or add protobuf definitions in `protos/`**  
   - Define new messages or services in files like `searcher.proto`, `bundle.proto`, `packet.proto`, `shared.proto`.
   - _Example:_  
     ```proto
     // protos/bundle.proto
     message Bundle {
       repeated Transaction transactions = 1;
     }
     ```

3. **Update build configuration**  
   - Edit `Cargo.toml` and `Cargo.lock` if dependencies or features change.
   - Update `build.rs` for custom build steps.
   - _Example:_  
     ```toml
     [dependencies]
     prost = "0.11"
     ```

### Feature Hardening and Configuration Docs
**Trigger:** When you want to improve robustness, configurability, and documentation for an existing feature.  
**Command:** `/harden-feature`

1. **Update core logic files in `src/`**  
   - Add support for new configuration options or environment variables.
   - Refactor code for better quality or error handling.
   - _Example:_  
     ```rust
     let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
     ```

2. **Add or update environment variable documentation in `.env.example`**  
   - Document all required and optional environment variables.
   - _Example:_  
     ```
     RPC_URL=https://api.mainnet-beta.solana.com
     PRIVATE_KEY=your_private_key_here
     ```

3. **Update `README.md` with new instructions**  
   - Add setup steps, usage instructions, and warnings.
   - _Example:_  
     ```
     ## Configuration
     Set the `RPC_URL` and `PRIVATE_KEY` environment variables before running the bot.
     ```

## Testing Patterns

- **Test File Pattern:**  
  Test files use the `*.test.*` naming convention.
  _Example:_  
  ```
  src/arb.test.rs
  src/executor.test.rs
  ```

- **Testing Framework:**  
  The specific framework is not detected, but Rust's built-in test framework is likely used.
  _Example:_  
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      #[test]
      fn test_arbitrage_logic() {
          // test implementation
      }
  }
  ```

## Commands
| Command        | Purpose                                                    |
|----------------|------------------------------------------------------------|
| /new-feature   | Scaffold and implement a new core feature or engine        |
| /harden-feature| Harden an existing feature with config and documentation   |
```

# Custom Instructions for Solana Trading Bot Agent

## Role & Goal
You are an expert Rust and Solana developer. Your goal is to implement high-performance transaction logic and account monitoring for a trading bot. You must operate autonomously, ensuring all code is idiomatic, asynchronous, and follows the strict safety requirements of the Solana SDK.

## Core Implementation Requirements

### 1. Transaction & Fee Management
- **Compute Budget:** Update the `send_transaction` function to include `ComputeBudgetInstruction::set_compute_unit_price`.
- **Priority Fees:** Set a default compute unit price of **100,000 micro-lamports per compute unit** to help transactions land during high network congestion, and make this value configurable via env/config.
- **Imports:** Ensure `solana_sdk::compute_budget::ComputeBudgetInstruction` is added to the relevant files.

### 2. Account & Transaction Monitoring
- **Filtering:** In `on_account_update`, first filter updates by `account.owner == WATCHED_PROGRAM`, and then apply an optional specific target account public key check if the strategy only cares about a known address.
- **Decoding:** Add a skeleton for decoding account data using the watched program's actual serialization format. If the program uses Borsh, add `borsh` (and `borsh-derive` when derives are needed) to `Cargo.toml` before deserializing raw bytes to read price or liquidity.
- **Log Scanning:** Update `on_transaction` to scan `tx.meta.log_messages` for specific strings like `"Swap"` or `"initialize"` to identify trading opportunities.

### 3. Technical Standards
- **Performance:** All logic must remain asynchronous (`async/await`).
- **Logging:** Use the `log` crate (e.g., `info!`, `error!`, `warn!`) for debugging and status updates. **Do not use `println!`**.
- **Constants:** Create a constant named `WATCHED_PROGRAM` for the Raydium or Pump.fun Program ID and use it as the primary `account.owner` filter in `on_account_update`.

## File-Specific Instructions: `executor.rs`
1. **Imports:** Add `use solana_sdk::compute_budget::ComputeBudgetInstruction;`.
2. **Transaction Logic:** Inside `send_transaction`, append the compute unit price instruction to the instructions vector before the transaction is signed, using the configurable default of `100_000` micro-lamports per compute unit when no override is provided.
3. **Filtering:** Implement the `WATCHED_PROGRAM` owner filter within the account update handler, with an optional target account pubkey check layered on top when the strategy requires a single known account.

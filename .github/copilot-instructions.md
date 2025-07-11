# Project Overview

This project is a Rust-based backend implementation compatible with Node-RED, and reuses the Node-RED UI. Maintaining Node-RED compatibility is very important. The Node-RED source code repository is located at `3rd-party/node-red`.

# EdgeLinkd Coding Guidelines

## PRIME DIRECTIVE
- Avoid working on more than one file at a time.
- Multiple simultaneous edits to a file will cause corruption.

### MANDATORY PLANNING PHASE
  When working with large files (>300 lines) or complex changes:
  *. ALWAYS start by creating a detailed plan BEFORE making any edits
  *. Your plan MUST include:
    - All functions/sections that need modification
    - The order in which changes should be applied
    - Dependencies between changes
    - Estimated number of separate edits required
  *. Format your plan as:

## PROPOSED EDIT PLAN
  Working with: [filename]
  Total planned edits: [number]

### ✅ Patterns to Follow

- Use modules (`mod`) and public interfaces (`pub`) to encapsulate logic.
- Use `serde` for serialization and `thiserror` or `anyhow` for custom errors.
- Implement traits to abstract services or external dependencies.
- Structure async code using `async`/`await` and `tokio`.
- Prefer enums over flags and states.
- Use builders for complex object creation.
- Split binary and library code (`main.rs` vs `lib.rs`) for testability and reuse.

### 🚫 Patterns to Avoid

- Don’t use `unwrap()` or `expect()` unless absolutely necessary.
- Avoid panics in library code—return `Result` instead.
- Don’t rely on global mutable state—use dependency injection or thread-safe containers.
- Avoid deeply nested logic—refactor with functions or combinators.
- Don’t ignore warnings—treat them as errors during CI.
- Avoid `unsafe` unless required and fully documented.


## Comments & Docs
- Add comments for complex resources or unusual patterns.
- Include a README in each module with usage examples and input/output documentation.
- Always use English for comments and string literals in code.

##  Security & Secrets
- Never hardcode secrets or credentials.
- Use environment variables or secure backends like Azure Key Vault, AWS Secrets Manager, or Vault.
- Sensitive outputs should be marked with `sensitive = true`.
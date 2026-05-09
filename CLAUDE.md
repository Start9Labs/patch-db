# CLAUDE.md

For AI developers (Claude Code, Copilot, etc.). See `CONTRIBUTING.md` for the doc map and contribution workflow.

## Operating rules

- **Wire format** — Rust and TS define `Revision`, `Dump`, and patch operations independently. Changes to one side must be mirrored in the other. See the cross-layer section in [CONTRIBUTING.md](CONTRIBUTING.md#making-changes).
- **Patch operations** — Only `add`, `remove`, and `replace` are used. The TS client does not implement `test`, `move`, or `copy`.
- **Immutable patch application** — The TS client applies patches by shallow-copying objects/arrays, not mutating in place. This is intentional for UI framework change detection.
- **`HasModel` derive** — Respects serde attributes (`rename_all`, `rename`, `flatten`, `tag`, `content`). Generated accessors follow the pattern `as_<field>()`, `as_<field>_mut()`, `into_<field>()`.
- **Error handling** — Rust uses `thiserror` with the `Error` enum in `core/src/lib.rs`. TS does not have formal error types.

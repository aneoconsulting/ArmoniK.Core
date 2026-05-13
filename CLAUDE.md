# ArmoniK.Core — Claude Code Guide

@AGENTS.md

## Claude-specific notes

- When exploring unfamiliar code, start from `Common/src/` for abstractions, then follow DI registrations to concrete implementations in `Adaptors/`.
- When writing tests, match the style of existing tests in the nearest `tests/` sibling directory.
- Do not run `just destroy` or destructive Terraform commands without explicit user confirmation.
- Integration tests require a running deployment — confirm infra is up before attempting them.

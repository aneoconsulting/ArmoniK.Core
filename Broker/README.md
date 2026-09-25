# ArmoniK Broker

In-memory task queue for ArmoniK, written in Rust: fairness between sessions (deficit round robin),
strict priorities (1 to 16) inside a session, leases renewed per consumer, and task/data affinity
(tasks are preferably given to the node that already holds their data).

- Design: `__docs__/broker-armonik-architecture-v0.49.md`
- Protocol v1 (REST, JSON, HTTP/1.1 and HTTP/2): `docs/protocol.md`
- Conformance vectors shared with the C# client: `conformance/affinity.json`
- C# client, loaded as a queue adapter: `Adaptors/Broker/`

No persistence: a restart loses the queue content. Tasks stay in the database; resume them by pausing
then resuming the affected sessions.

## Build and test

```bash
cargo test                        # unit, property and protocol tests
UPDATE_VECTORS=1 cargo test conformance   # regenerate conformance vectors after an intended change
cargo run -- --help               # all settings, also available as BROKER_* environment variables
just buildBroker               # docker image (from the repository root)
just queue=broker build-deploy # full deployment
```

The C# adapter tests (`Adaptors/Broker/tests`) start the debug binary; they look for it in
`BROKER_BINARY`, `$HOME/.cache/armonik-broker-target/debug` or `Broker/target/debug`.

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

## Benchmarks

```bash
cargo bench                        # scheduler alone (benches/scheduler.rs) and HTTP chain (benches/http.rs)
benches/check-floors.sh            # CI floors (benches/floors.json) against the last run
benches/compare-hashers.sh 3       # hashers of the hot-path tables (src/hashing.rs), 3 rotating rounds
```

Numbers only hold on a quiet Linux machine: in a VM (WSL, CI runner) the host can slow a core several
times over without the guest seeing it. The server hashes with foldhash; the `hash-sip`, `hash-fx`
or `hash-ahash` feature selects another hasher at build time, for comparison only.

The C# adapter tests (`Adaptors/Broker/tests`) start the debug binary; they look for it in
`BROKER_BINARY`, `$HOME/.cache/armonik-broker-target/debug` or `Broker/target/debug`.

# ArmoniK Broker — protocol v1

Contract between the `Broker/` server (Rust) and its `Adaptors/Broker/` client (C#).
There is no code generation: this document and the vectors in `Broker/conformance/` are the only
source of truth.

## 1. Transport

- HTTP/1.1 and HTTP/2 (h2 through ALPN, or cleartext h2c for development). Same API.
- Optional TLS; mutual TLS when a client certificate authority is configured. No per-operation
  authorization.
- Bodies in `application/json`, UTF-8. Body sizes are capped by `max_body_bytes` (64 KiB by default).
- Every response carries the **`X-Broker-Epoch`** header: an unsigned 32-bit integer in decimal, drawn
  at random at startup. A change means that the content of the queue was lost.

## 2. Version

All routes are prefixed with `/v1`. A single version is served at a time. A request to another `/vN`
prefix gets `404` with the `unsupported-version` type. The broker is a single instance whose queue is
lost on restart (§1): there is no rolling upgrade to go through, and the client treats this `404` like
any final error.

## 3. Value conventions

| Value | Rule |
|---|---|
| Partition, fairness key, node identifier | non-empty UTF-8 string, at most 100 bytes |
| Task identifier | non-empty UTF-8 string, at most 59 bytes inline, up to 512 bytes through overflow |
| Priority | integer from 1 to 16, 16 = most urgent. Out of range: `409 invalid-priority` |
| Durations | milliseconds (`*_ms`), unsigned integers |
| Sizes | bytes (`*_bytes`), unsigned 64-bit integers |
| Encoded size | integer from 0 to 255 (§8.3); 0 = absent |
| Identifier hash | unsigned 32-bit integer (§8.2) |
| Token | opaque string; the client must neither build nor interpret it |

Unknown fields are ignored. Missing or mistyped required fields: `400 malformed`.

## 4. Errors

Body in `application/problem+json` (RFC 9457), for diagnostics:

```json
{ "type": "urn:armonik:broker:backpressure", "title": "backpressure", "status": 429,
  "detail": "hard memory threshold reached" }
```

**The client decides on the status alone**: `429` and `503` are retried, any other error status is
final. The type and the detail are only logged.

| Type (`urn:armonik:broker:…`) | Status | Meaning | Client behavior |
|---|---|---|---|
| `malformed` | 400 | invalid body, unreadable token, value out of format | client bug: log, fail |
| `unsupported-version` | 404 | version prefix not served (§2) | fail |
| `not-found` | 404 | unknown route | client bug: fail |
| `invalid-priority` | 409 | priority outside 1 to 16 | reject the submission |
| `partition-limit` | 409 | maximum number of partitions reached | fail: it is a configuration limit |
| `payload-too-large` | 413 | body beyond `max_body_bytes` | split the batch (§6.1) |
| `backpressure` | 429 | hard memory threshold, key pool full | wait `Retry-After`, replay the whole batch |
| `overloaded` | 503 | actor queue full, too many concurrent requests | wait `Retry-After`, replay |
| `shutting-down` | 503 | shutdown in progress | wait `Retry-After`, replay |

`429` and `503` carry `Retry-After` (seconds). An error status is final: **nothing was applied**, so a
replay creates no duplicate. An enqueue batch is atomic.

Delivery is **at least once**: a request whose response is lost (disconnection, timeout) is replayed by
the client although it may have been applied, and a replayed enqueue may then enqueue the same tasks a
second time. Core tolerates these duplicates, as with the other queues.

## 5. Client behavior

The protocol is stateless: no registration, no session identifier. Each request carries everything the
server needs.

- **Retry**: `429`, `503`, network errors and timeouts are retried, with an exponential backoff from
  100 ms to 10 s with jitter, replaced by `Retry-After` when the server provides it.
- **Lease**: the client renews the lease of each message it holds at a third of `lease_ms` at most
  (§6.3), and stops renewing it **before** settling it: if the settlement fails, the lease expires and
  the message is delivered again.
- **Settlement**: an ack or nack the server reports as ignored (§6.4) is logged as a warning by the C#
  client: its task may run again.
- **Unavailability**: while the broker is unavailable, a `pull` of the C# client **returns an empty
  list** and stays healthy; it does not propagate the error to the Pollster.
- **Restart**: a change of `X-Broker-Epoch` is logged; the tasks submitted before it must be resumed
  (pause then resume their sessions). Tokens of the previous epoch can still be settled: they get a
  silent success.

## 6. Operations

### 6.1 Enqueue — `POST /v1/partitions/{partition}/messages`

Homogeneous batch: partition, key and priority are given once for the whole batch. The partition is
created if it does not exist.

```json
{ "key": "session-42", "priority": 5,
  "items": [ { "task_id": "0f8c…###1" },
             { "task_id": "7a1e…",
               "affinity": { "hashes": [123, 456], "sizes": [40, 12], "dep_count": 2, "total_size": 41 } } ] }
```

- `items`: 1 to `max_batch_items` items (derived from `max_body_bytes`, about 150 by default). Beyond,
  or if the body exceeds `max_body_bytes`: `413`, and the client splits the batch in two and sends each
  half again.
- `affinity` (optional): `hashes` and `sizes` of the same length, at most 8 (§8.1); `dep_count`: total
  number of dependencies, saturated at 65535; `total_size`: encoded size of the sum of the sizes.
- `delay_ms` (optional, for the whole batch): delayed visibility of the batch, at most 24 h.

Response `200`:

```json
{ "accepted": 2, "occupancy": "normal" }
```

`occupancy` is `normal` or `high` (soft memory threshold exceeded, informative).

### 6.2 Pull — `POST /v1/partitions/{partition}/pull`

```json
{ "max": 1, "wait_ms": 600000,
  "node": { "id": "node-17", "cache_capacity_bytes": 10737418240,
            "fetch_fixed_cost_us": 3000, "fetch_throughput_bytes_per_s": 1000000000 } }
```

- `max`: at least 1, **capped** by `max_pull` (64 by default); `0` gives `400`.
- `wait_ms`: **capped** by `max_wait_ms` (10 min by default).
- `node` and each of its fields are optional; without `node.id`, affinity is inactive for this pull.
  The server keeps the last declaration of each node, and forgets it after `node_forget_ms` without a
  pull from it.
- The partition is created if it does not exist.
- **Partial** return as soon as at least one message is available.

Response `200`:

```json
{ "lease_ms": 30000,
  "messages": [ { "token": "AAAB…", "task_id": "0f8c…###1", "attempts": 1 } ] }
```

Each delivered message has its own lease of `lease_ms` from its delivery, extended only by a renew that
names it (§6.3). A broken connection requeues nothing: only the lease counts.

`204` without a body if the wait expires. On a clean shutdown of the broker, waiting pulls get `204`.

### 6.3 Renew — `POST /v1/renew`

```json
{ "tokens": [ "AAAB…", "AAAC…" ] }
```

Extends by `lease_ms` the lease of the **named** messages, and only those, whatever their partition. A
call may name several; the C# adapter renews each message with its own heartbeat. A message that is not
named is not renewed and goes back to the queue when its lease expires: this is what recovers a pull
response lost on the way or an abandoned settlement. Response `200`:

```json
{ "lease_ms": 30000, "unknown": [ "AAAC…" ] }
```

`unknown` lists the tokens that no longer designate a current delivery (already settled, expired, from
another epoch): the client stops renewing them. An unreadable token gives `400`.

### 6.4 Ack — `POST /v1/ack`

```json
{ "items": [ { "token": "AAAB…", "outputs": { "hashes": [789], "sizes": [33] } } ] }
```

`outputs` (optional): outputs produced, selected with the same rule as the dependencies (§8.1).
Response `200`: `{ "applied": 1, "ignored": 0 }`.

**Safety rule.** A token never acknowledges another message than the one of its delivery. A well-formed
token that no longer designates a current delivery — acknowledged, delivered again, other epoch,
unknown slot — is **ignored with success** and counted in `ignored`. Only an unreadable token gives
`400`.

### 6.5 Nack — `POST /v1/nack`

```json
{ "items": [ { "token": "AAAB…", "policy": "requeue" },
             { "token": "AAAC…", "policy": "delay", "delay_ms": 5000 },
             { "token": "AAAD…", "policy": "backoff" } ] }
```

- `requeue` (default): back to the queue at once, at the tail of its priority.
- `delay`: back to the queue after `delay_ms` (at most 24 h).
- `backoff`: delay of `min(backoff_base_ms × 2^(attempts-1), backoff_max_ms)`, that is 1 s × 2^n capped
  at 60 s by default.

Same safety rule as the ack. Response `200`: `{ "applied": n, "ignored": m }`.

### 6.6 Administration and diagnostics

| Route | Response |
|---|---|
| `GET /v1/partitions/{p}/stats?top=N&key=K` | `{ "ready": n, "in_flight": n, "delayed": n, "oldest_ready_age_ms": n, "waiters": n, "keys": [ { "key": "…", "ready": n, "in_flight": n, "oldest_ready_age_ms": n } ] }` — `keys`: the key `K` if given, otherwise the `N` largest (10 by default) |
| `GET /v1/partitions/{p}/leases?limit=N` | `{ "leases": [ { "token", "task_id", "node_id", "dispatched_age_ms", "attempts" } ] }`, from the oldest delivered to the most recent |
| `GET /v1/partitions/{p}/peek` | `{ "heads": [ { "key", "priority", "task_id" } ] }`: head of each non-empty key and priority pair, at most 1000 |
| `GET /v1/messages/{token}` | `{ "state": "in-flight", "task_id", "partition", "node_id", "dispatched_age_ms", "attempts" }` or `{ "state": "stale" }` |
| `DELETE /v1/partitions/{p}` | `204`; deletes messages, leases and keys; waiting pulls get `204` and the tokens issued become stale |
| `GET /v1/health` | `200 { "status": "ok" }`, or `503` during shutdown |
| `GET /metrics` | Prometheus text format |

An unknown partition read returns zero counters, not `404`.

## 7. Token

Opaque to the client. The server encodes `epoch`, `partition`, `slot` and `generation` in it and
validates all of them; the generation changes each time the message leaves the in-flight state. The
current form (base64url of 16 bytes) is not part of the contract.

## 8. Affinity structure

Computed by the producer (Core); the broker only sees the result. The vectors of
`conformance/affinity.json` set the expected behavior; Rust and C# must reproduce them exactly.

### 8.1 Selection of the 8 slots

Input: list of (identifier, size in bytes) pairs. Duplicate identifiers are merged (the first size is
kept).

1. Compute `h = hash(id)` (§8.2) for each dependency.
2. **Half by size**: sort by decreasing size, then by increasing `h`, then by increasing identifier
   (ordinal); take the first 4.
3. **Half by hash**: among the remaining dependencies, sort by increasing `h`, then by increasing
   identifier; complete up to 8 in total.
4. Emit in order: the half by size, then the half by hash. `sizes[i] = encode(size)` (§8.3), never 0
   for a real dependency.
5. `dep_count = min(number of distinct dependencies, 65535)`;
   `total_size = encode(sum of the sizes, saturated at 2^64-1)`.

No dependency: `affinity` absent.

### 8.2 Hash

```
hash(id) = XXH32(utf8(id), seed = 0)
```

XXH32 is the standard xxHash algorithm, available in both languages (`System.IO.Hashing.XxHash32` in
.NET, `xxhash-rust` in Rust); its mixing ensures that close identifiers (time-ordered UUID v7,
counters) do not give close hashes.

### 8.3 Logarithmic encoding of sizes

Minifloat of the value `v = s + 1`: the exponent and the two bits after the leading one, that is four
steps per octave (at most 25 % between steps); integer arithmetic only.

```
encode(s):
    v = s + 1                      (saturated at 2^64 - 1)
    e = 63 - leading_zeros(v)      // floor(log2 v)
    m = (e ≥ 2 ? v >> (e - 2) : v << (2 - e)) & 3
    return min(255, 1 + 4·e + m)
decode(c) ≈ (4 + m) · 2^(e - 2) - 1  with e = (c - 1) / 4, m = (c - 1) mod 4
                                   // lower bound of the step; indicative use (scoring), never compared between implementations
```

`encode(0) = 1`, `encode(1) = 5`, non-decreasing encoding; 255 is only reached beyond 2^63 bytes.

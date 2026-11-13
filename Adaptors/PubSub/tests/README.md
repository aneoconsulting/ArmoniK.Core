# ArmoniK.Core.Adapters.PubSub.Tests

Tests specific to the Pub/Sub queue adapter. They run against the Pub/Sub emulator.

## Prerequisites

A Pub/Sub emulator reachable through the `PUBSUB_EMULATOR_HOST` environment variable (default: `localhost:8085`).
It can be deployed alone with Terraform:

```bash
just queue=pubsub deployTargetQueue
```

The adapter source project must be built first, so that the test fixture can load it dynamically:

```bash
dotnet build Adaptors/PubSub/src/
```

## Run

```bash
dotnet test Adaptors/PubSub/tests/
```

## Emulator limitation

When a topic has a message retention, the emulator delivers the messages retained by the topic to a subscription
created after them, even without seek. GCP does not: only messages published after the subscription creation are
delivered, unless the subscription is seeked back. The emulator therefore cannot prove that a seek is required.

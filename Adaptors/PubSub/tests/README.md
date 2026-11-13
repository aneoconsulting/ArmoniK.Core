# ArmoniK.Core.Adapters.PubSub.Tests

Tests specific to the Pub/Sub queue adapter. They run against the Pub/Sub emulator.

## Prerequisites

A Pub/Sub emulator reachable through the `PUBSUB_EMULATOR_HOST` environment variable (default: `localhost:8085`).
It can be deployed alone with Terraform:

```bash
just queue=pubsub deployTargetQueue
```

The adapter is built along with the test project and loaded dynamically from its output directory.

## Run

```bash
dotnet test Adaptors/PubSub/tests/
```

## Emulator limitation

When a topic has a message retention, the emulator delivers the messages retained by the topic to a subscription
created after them. GCP does not: only the messages published after the subscription creation are delivered.
Tests that need the GCP behavior create their topic without message retention.

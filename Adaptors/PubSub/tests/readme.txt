To run tests from ArmoniK.Core.Adapters.PubSub.Tests.csproj you need a Pub/Sub emulator reachable
through the PUBSUB_EMULATOR_HOST environment variable (default: localhost:8085).

#### start the emulator with the queue-only Terraform deployment ####
just queue=pubsub deployTargetQueue

The adapter source project must be built first so the test fixture can dynamically load it:
    dotnet build Adaptors/PubSub/src/

Then launch the tests:
    dotnet test Adaptors/PubSub/tests/

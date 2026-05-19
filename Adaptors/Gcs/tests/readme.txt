To run tests from ArmoniK.Core.Adapters.Gcs.Tests.csproj you need to have a Google Cloud Storage
endpoint reachable on http://127.0.0.1:4443. For local development, fake-gcs-server is used:

#### start fake-gcs-server and pre-create the test bucket ####
docker run --rm -d --name fake-gcs -p 4443:4443 fsouza/fake-gcs-server \
    -scheme http -public-host 127.0.0.1:4443

# create the bucket the tests expect
curl -X POST -H "Content-Type: application/json" \
    -d '{"name":"armonik-test-bucket"}' \
    http://127.0.0.1:4443/storage/v1/b?project=test-project

# (optional) browse the bucket contents at:
# http://127.0.0.1:4443/storage/v1/b/armonik-test-bucket/o

The adapter source project must be built first so the test fixture can dynamically load it:
    dotnet build Adaptors/Gcs/src/

Then launch the tests:
    dotnet test Adaptors/Gcs/tests/

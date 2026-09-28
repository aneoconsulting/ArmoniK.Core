To run tests from ArmoniK.Core.Adapters.S3.Tests.csproj you need to have an S3 server up locally

For example you can start a SeaweedFS S3 server from docker this way (the bucket is created at startup) :

docker run --rm -p 9000:9000 -e AWS_ACCESS_KEY_ID=seaweedfs -e AWS_SECRET_ACCESS_KEY=seaweedfs chrislusf/seaweedfs:4.47 mini -dir=/data -s3.port=9000 -bucket=armonik-bucket -master.telemetry=false

or with the same configuration as the CI :

just object=seaweedfs deployTargetObject

SeaweedFS checks request signatures, the credentials must match the ones used by the tests (seaweedfs/seaweedfs).

Now you can launch your tests from : 'ArmoniK.Core.Adapters.S3.Tests.csproj' succesfuly

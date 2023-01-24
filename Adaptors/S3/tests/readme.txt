To run tests from ArmoniK.Core.Adapters.S3.Tests.csproj you need to have an S3 server up locally

For example you can start minio S3 server from docker this way :

#### create minio server and create bucket in 1 command ####
docker run --rm -p 9000:9000 -p 9001:9001 --entrypoint "/bin/bash" quay.io/minio/minio "-c" "mkdir -p /data/miniobucket && minio server /data --console-address :9001"

#### another way : create minio server and create bucket in 2 commands ####
# rem: for this way, you will need aws cli installed
docker run --rm -p 9000:9000 -p 9001:9001 quay.io/minio/minio server /data --console-address ":9001"
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin aws s3 mb s3://miniobucket  --endpoint-url http://127.0.0.1:9000

you can check from : http://127.0.0.1:9001/ login:minioadmin password:minioadmin
you must see an empty bucket 'miniobucket' -> minio S3 server is up and well configured for tests

Now you can launch your tests from : 'ArmoniK.Core.Adapters.S3.Tests.csproj' succesfuly
rem: if you check again in http://127.0.0.1:9001/ you will see your bucket filled with some datas


locals {
  test-cmd = <<-EOF
    CMD bash -c exec 3<>"/dev/tcp/localhost/1080"
    echo -en "GET /liveness HTTP/1.1\r\nHost: localhost:1080\r\nConnection: close\r\n\r\n">&3 &
    grep Healthy <&3 &>/dev/null || exit 1
    EOF
}

resource "docker_image" "pollingagent" {
  name         = "${var.docker_image}:${var.core_tag}"
  keep_locally = true
}

resource "docker_container" "pollingagent" {
  name  = var.container_name
  image = docker_image.pollingagent.image_id

  networks_advanced {
    name = var.network
  }

  env = [
    "Components__TableStorage=ArmoniK.Adapters.MongoDB.TableStorage",
    "MongoDB__Host=database",
    "MongoDB__Port=27017",
    "MongoDB__DatabaseName=database",
    "MongoDB__MaxConnectionPoolSize=500",
    "MongoDB__TableStorage__PollingDelayMin=00:00:01",
    "MongoDB__TableStorage__PollingDelayMax=00:00:10",
    "Components__ObjectStorage=ArmoniK.Adapters.Redis.ObjectStorage",
    "Redis__EndpointUrl=object:6379",
    "Pollster__MaxErrorAllowed=-1",
    "InitWorker__WorkerCheckRetries=10",
    "InitWorker__WorkerCheckDelay=00:00:10",
    "Serilog__MinimumLevel=Information",
    "Zipkin__Uri=${var.zipkin_uri}",
    "ASPNETCORE_ENVIRONMENT=Development",
    "Components__QueueStorage=ArmoniK.Adapters.Amqp.QueueStorage",
    "Amqp__User=admin",
    "Amqp__Password=admin",
    "Amqp__Host=queue",
    "Amqp__Port=5672",
    "Amqp__Scheme=AMQP",
    "Amqp__MaxPriority=10",
    "Amqp__MaxRetries=10",
    "Amqp__LinkCredit=2",
    "Amqp__PartitionId=TestPartition${var.replica_counter}"
  ]

  log_driver = var.log_driver_name

  log_opts = {
    fluentd-address = var.log_driver_address
  }

  ports {
    internal = 1080
    external = 9980 + var.replica_counter
  }

  mounts {
    type   = "volume"
    target = "/cache"
    source = var.socket_vol
  }

  healthcheck {
    test         = split(" ", local.test-cmd)
    interval     = "5s"
    timeout      = "3s"
    start_period = "20s"
    retries      = 5
  }
}
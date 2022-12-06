resource "docker_container" "submitter" {
  name  = "armonik.control.submitter"
  image = "${var.armonik-submitter-image}:${var.core-tag}"

  networks_advanced {
    name = docker_network.armonik-net.name
  }
  networks_advanced {
    name = docker_network.armonik-backend.name
  }
  networks_advanced {
    name = docker_network.armonik-monitoring.name
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
    "Submitter__DefaultPartition=TestPartition0",
    "Serilog__MinimumLevel=Information",
    "Zipkin__Uri=http://zipkin:9411/api/v2/spans",
    "ASPNETCORE_ENVIRONMENT=Development",
    "Components__QueueStorage=ArmoniK.Adapters.Amqp.QueueStorage",
    "Amqp__User=admin",
    "Amqp__Password=admin",
    "Amqp__Host=queue",
    "Amqp__Port=5672",
    "Amqp__Scheme=AMQP",
    "Amqp__MaxPriority=10",
    "Amqp__MaxRetries=10",
    "Amqp__LinkCredit=2"
  ]

  log_driver = "fluentd"

  log_opts = {
    fluentd-address = "127.0.0.1:24224"
  }

  ports {
    internal = 1080
    external = 5001
  }

  ports {
    internal = 1081
    external = 5011
  }

  depends_on = [
    docker_container.database,
    docker_container.queue,
    docker_container.object,
    docker_container.fluentbit
  ]
}

resource "docker_container" "pollingagent" {
  name  = "armonik.compute.pollingagent"
  image = "${var.armonik-pollingagent-image}:${var.core-tag}"

  networks_advanced {
    name = docker_network.armonik-backend.name
  }
  networks_advanced {
    name = docker_network.armonik-monitoring.name
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
    "Zipkin__Uri=http://zipkin:9411/api/v2/spans",
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
    "Amqp__PartitionId=TestPartition0"
  ]

  log_driver = "fluentd"

  log_opts = {
    fluentd-address = "127.0.0.1:24224"
  }

  ports {
    internal = 1080
    external = 9980
  }

  mounts {
    type   = "volume"
    target = "/cache"
    source = docker_volume.socket-vol.name
  }

  depends_on = [
    docker_container.database,
    docker_container.queue,
    docker_container.object,
    docker_container.fluentbit,
    docker_container.zipkin
  ]
}

resource "docker_container" "worker" {
  name  = "armonik.compute.worker"
  image = "${var.armonik-worker-image}:${var.core-tag}"

  networks_advanced {
    name = docker_network.armonik-net.name
  }
  networks_advanced {
    name = docker_network.armonik-backend.name
  }
  networks_advanced {
    name = docker_network.armonik-monitoring.name
  }

  env = [
    "ASPNETCORE_ENVIRONMENT=Development",
    "Serilog__Properties__Application=ArmoniK.Compute.Worker",
    "Serilog__MinimumLevel=Information"
  ]

  log_driver = "fluentd"

  log_opts = {
    fluentd-address = "127.0.0.1:24224"
  }

  mounts {
    type   = "volume"
    target = "/cache"
    source = docker_volume.socket-vol.name
  }

  depends_on = [
    docker_container.database,
    docker_container.queue,
    docker_container.object,
    docker_container.fluentbit
  ]
}

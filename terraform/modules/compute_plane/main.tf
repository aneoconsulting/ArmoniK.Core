locals {
  test-cmd = <<-EOF
    exec 3<>"/dev/tcp/localhost/1080"
    echo -en "GET /liveness HTTP/1.1\r\nHost: localhost:1080\r\nConnection: close\r\n\r\n">&3 &
    grep Healthy <&3 &>/dev/null || exit 1
    EOF
}

resource "docker_volume" "socket_vol" {
  name = "socket_vol${var.replica_counter}"
}

resource "docker_image" "worker" {
  count        = var.use_local_image ? 0 : 1
  name         = "${var.worker_image}:${var.core_tag}"
  keep_locally = true
}

module "worker_local" {
  count           = var.use_local_image ? 1 : 0
  source          = "../build_image"
  use_local_image = var.use_local_image
  image_name      = "submitter_local"
  context_path    = "${path.root}../"
  dockerfile_path = "${path.root}../Tests/HtcMock/Server/src" # TODO: Make this a variable ot change worker type
}

resource "docker_container" "worker" {
  name  = var.worker_container_name
  image = var.use_local_image ? module.worker_local[0].image_id : docker_image.worker[0].image_id

  networks_advanced {
    name = var.network
  }

  env = [
    "ASPNETCORE_ENVIRONMENT=Development",
    "Serilog__Properties__Application=ArmoniK.Compute.Worker",
    "Serilog__MinimumLevel=Information"
  ]

  log_driver = var.log_driver.name

  log_opts = {
    fluentd-address = var.log_driver.address
  }

  ports {
    internal = 1080
    external = 1080 + var.replica_counter
  }

  mounts {
    type   = "volume"
    target = "/cache"
    source = docker_volume.socket_vol.name
  }
}

resource "docker_image" "polling_agent" {
  count        = var.use_local_image ? 0 : 1
  name         = "${var.polling_agent_image}:${var.core_tag}"
  keep_locally = true
}

module "pollingagent_local" {
  count           = var.use_local_image ? 1 : 0
  source          = "../build_image"
  use_local_image = var.use_local_image
  image_name      = "submitter_local"
  context_path    = "${path.root}../"
  dockerfile_path = "${path.root}../Compute/PollingAgent/src/"
}

resource "docker_container" "polling_agent" {
  name  = var.polling_agent_container_name
  image = var.use_local_image ? module.pollingagent_local[0].image_id : docker_image.polling_agent[0].image_id

  networks_advanced {
    name = var.network
  }

  env = [
    "Components__TableStorage=ArmoniK.Adapters.MongoDB.TableStorage",
    "MongoDB__Host=${var.db_driver.name}",
    "MongoDB__Port=${var.db_driver.port}",
    "MongoDB__DatabaseName=${var.db_driver.name}",
    "MongoDB__MaxConnectionPoolSize=500",
    "MongoDB__TableStorage__PollingDelayMin=00:00:01",
    "MongoDB__TableStorage__PollingDelayMax=00:00:10",
    "Components__ObjectStorage=ArmoniK.Adapters.Redis.ObjectStorage",
    "Redis__EndpointUrl=${var.object_driver.address}",
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

  log_driver = var.log_driver.name

  log_opts = {
    fluentd-address = var.log_driver.address
  }

  ports {
    internal = 1080
    external = 9980 + var.replica_counter
  }

  mounts {
    type   = "volume"
    target = "/cache"
    source = docker_volume.socket_vol.name
  }

  healthcheck {
    test         = concat(["CMD", "bash", "-c"], split(" ", local.test-cmd))
    interval     = "5s"
    timeout      = "3s"
    start_period = "20s"
    retries      = 5
  }
}
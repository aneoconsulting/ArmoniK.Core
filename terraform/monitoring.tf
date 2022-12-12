resource "docker_image" "fluentbit" {
  name         = var.log_driver
  keep_locally = true
}

resource "docker_container" "fluentbit" {
  name    = "fluentbit"
  image   = docker_image.fluentbit.image_id
  restart = "always"

  networks_advanced {
    name = docker_network.armonik_net.name
  }

  mounts {
    type   = "bind"
    target = "/fluent-bit/etc"
    source = abspath("./fluent-bit/etc")
  }

  mounts {
    type   = "bind"
    target = "/logs"
    source = abspath("./logs")
  }

  ports {
    internal = 24224
    external = 24224
    ip       = "127.0.0.1"
  }

  ports {
    internal = 24224
    external = 24224
    protocol = "udp"
    ip       = "127.0.0.1"
  }

  depends_on = [
    docker_container.seq
  ]
}

resource "docker_image" "seq" {
  name         = var.seq_image
  keep_locally = true
}

resource "docker_container" "seq" {
  name  = "seq"
  image = docker_image.seq.image_id

  networks_advanced {
    name = docker_network.armonik_net.name
  }

  env = [
    "ACCEPT_EULA=Y"
  ]

  ports {
    internal = 80
    external = 80
  }

  ports {
    internal = 5341
    external = 5341
  }
}

resource "docker_image" "zipkin" {
  name         = var.zipkin_image
  keep_locally = true
}

resource "docker_container" "zipkin" {
  name  = "zipkin"
  image = docker_image.zipkin.image_id

  networks_advanced {
    name = docker_network.armonik_net.name
  }

  ports {
    internal = 9411
    external = 9411
  }
}

resource "docker_image" "metrics" {
  count        = var.use_local_image ? 0 : 1
  name         = "${var.armonik_metrics_image}:${var.core_tag}"
  keep_locally = true
}

module "metrics_local" {
  source          = "./modules/localImage"
  use_local_image = var.use_local_image
  image_name      = "metrics_local"
  context_path    = "../"
  dockerfile_path = "../Control/Metrics/src/"
}

resource "docker_container" "metrics" {
  name  = "armonik.control.metrics"
  image = var.use_local_image ? module.metrics_local.image_id : docker_image.metrics[0].image_id

  networks_advanced {
    name = docker_network.armonik_net.name
  }

  env = [
    "Components__TableStorage=ArmoniK.Adapters.MongoDB.TableStorage",
    "MongoDB__Host=database",
    "MongoDB__Port=27017",
    "MongoDB__DatabaseName=database",
    "MongoDB__MaxConnectionPoolSize=500",
    "MongoDB__TableStorage__PollingDelayMin=00:00:01",
    "MongoDB__TableStorage__PollingDelayMax=00:00:10",
    "Serilog__MinimumLevel=Information",
    "ASPNETCORE_ENVIRONMENT=Development"
  ]

  log_driver = "fluentd"

  log_opts = {
    fluentd-address = "127.0.0.1:24224"
  }

  ports {
    internal = 1080
    external = 5002
  }

  depends_on = [
    docker_container.database,
    docker_container.queue,
    docker_container.object,
    docker_container.fluentbit
  ]
}

resource "docker_image" "partition_metrics" {
  count        = var.use_local_image ? 0 : 1
  name         = "${var.armonik_partition_metrics_image}:${var.core_tag}"
  keep_locally = true
}

module "partition_metrics_local" {
  source          = "./modules/localImage"
  use_local_image = var.use_local_image
  image_name      = "partition_metrics_local"
  context_path    = "../"
  dockerfile_path = "../Control/PartitionMetrics/src/"
}

resource "docker_container" "partition_metrics" {
  name  = "armonik.control.partition_metrics"
  image = var.use_local_image ? module.partition_metrics_local.image_id : docker_image.partition_metrics[0].image_id

  networks_advanced {
    name = docker_network.armonik_net.name
  }

  env = [
    "Components__TableStorage=ArmoniK.Adapters.MongoDB.TableStorage",
    "MongoDB__Host=database",
    "MongoDB__Port=27017",
    "MongoDB__DatabaseName=database",
    "MongoDB__MaxConnectionPoolSize=500",
    "MongoDB__TableStorage__PollingDelayMin=00:00:01",
    "MongoDB__TableStorage__PollingDelayMax=00:00:10",
    "Serilog__MinimumLevel=Information",
    "MetricsExporter__Host=http://armonik.control.metrics",
    "MetricsExporter__Port=1080",
    "MetricsExporter__Path=/metrics"
  ]

  log_driver = "fluentd"

  log_opts = {
    fluentd-address = "127.0.0.1:24224"
  }

  ports {
    internal = 1080
    external = 5003
  }

  depends_on = [
    docker_container.database,
    docker_container.queue,
    docker_container.object,
    docker_container.fluentbit,
    docker_container.metrics
  ]
}

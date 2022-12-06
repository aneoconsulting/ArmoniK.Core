
resource "docker_container" "fluentbit" {
  name    = "fluentbit"
  image   = var.log-driver
  restart = "always"

  networks_advanced {
    name = docker_network.armonik-monitoring.name
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

  depends_on = [
    docker_container.seq
  ]
}

resource "docker_container" "seq" {
  name  = "seq"
  image = var.seq-image

  networks_advanced {
    name = docker_network.armonik-monitoring.name
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

resource "docker_container" "zipkin" {
  name  = "zipkin"
  image = var.zipkin-image

  networks_advanced {
    name = docker_network.armonik-monitoring.name
  }

  ports {
    internal = 9411
    external = 9411
  }
}

resource "docker_container" "metrics" {
  name  = "armonik.control.metrics"
  image = "${var.armonik-metrics-image}:${var.core-tag}"

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

resource "docker_container" "partition-metrics" {
  name  = "armonik.control.partition-metrics"
  image = "${var.armonik-partition-metrics-image}:${var.core-tag}"

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

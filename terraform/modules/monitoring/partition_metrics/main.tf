resource "docker_image" "partition_metrics" {
  count        = var.use_local_image ? 0 : 1
  name         = "${var.image}:${var.tag}"
  keep_locally = true
}

module "partition_metrics_local" {
  count           = var.use_local_image ? 1 : 0
  source          = "../../build_image"
  use_local_image = var.use_local_image
  image_name      = "partition_metrics_local"
  context_path    = "${path.root}../"
  dockerfile_path = "${path.root}../Control/PartitionMetrics/src/"
}

resource "docker_container" "partition_metrics" {
  name  = "armonik.control.partition_metrics"
  image = var.use_local_image ? module.partition_metrics_local[0].image_id : docker_image.partition_metrics[0].image_id

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
    "Serilog__MinimumLevel=Information",
    "MetricsExporter__Host=http://armonik.control.metrics",
    "MetricsExporter__Port=1080",
    "MetricsExporter__Path=/metrics"
  ]

  log_driver = var.log_driver.name

  log_opts = {
    fluentd-address = var.log_driver.address
  }

  ports {
    internal = 1080
    external = var.exposed_port
  }
}
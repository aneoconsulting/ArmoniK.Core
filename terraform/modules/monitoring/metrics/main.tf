resource "docker_image" "metrics" {
  count        = var.use_local_image ? 0 : 1
  name         = "${var.image}:${var.tag}"
  keep_locally = true
}

module "metrics_local" {
  count           = var.use_local_image ? 1 : 0
  source          = "../../build_image"
  use_local_image = var.use_local_image
  image_name      = "metrics_local"
  context_path    = "${path.root}../"
  dockerfile_path = "${path.root}../Control/Metrics/src/"
}

resource "docker_container" "metrics" {
  name  = "armonik.control.metrics"
  image = var.use_local_image ? module.metrics_local[0].image_id : docker_image.metrics[0].image_id

  networks_advanced {
    name = var.network
  }

  env = [
    "Components__TableStorage=ArmoniK.Adapters.MongoDB.TableStorage",
    "MongoDB__Host=${var.db_driver.name}",
    "MongoDB__Port=${var.db_driver.port}",
    "MongoDB__DatabaseName=${var.db_driver.name}",
    "MongoDB__MaxConnectionPoolSize=${var.mongodb_params.max_connection_pool_size}",
    "MongoDB__TableStorage__PollingDelayMin=${var.mongodb_params.min_polling_delay}",
    "MongoDB__TableStorage__PollingDelayMax=${var.mongodb_params.max_polling_delay}",
    "Serilog__MinimumLevel=Information",
    "ASPNETCORE_ENVIRONMENT=Development"
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
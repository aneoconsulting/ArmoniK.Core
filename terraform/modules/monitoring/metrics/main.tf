resource "docker_image" "metrics" {
  count        = var.use_local_image ? 0 : 1
  name         = "${var.image}:${var.tag}"
  keep_locally = true
}

module "metrics_local" {
  source          = "../../localImage"
  use_local_image = var.use_local_image
  image_name      = "metrics_local"
  context_path    = "${path.root}../"
  dockerfile_path = "${path.root}../Control/Metrics/src/"
}

resource "docker_container" "metrics" {
  name  = "armonik.control.metrics"
  image = var.use_local_image ? module.metrics_local.image_id : docker_image.metrics[0].image_id

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
    "Serilog__MinimumLevel=Information",
    "ASPNETCORE_ENVIRONMENT=Development"
  ]

  log_driver = var.log_driver_name

  log_opts = {
    fluentd-address = var.log_driver_address
  }

  ports {
    internal = 1080
    external = var.exposed_port
  }
}
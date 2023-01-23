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
  context_path    = "${path.root}/../"
  dockerfile_path = "${path.root}/../Control/PartitionMetrics/src/"
}

resource "docker_container" "partition_metrics" {
  name  = "armonik.control.partition_metrics"
  image =  one(concat(module.partition_metrics_local, docker_image.partition_metrics)).image_id

  networks_advanced {
    name = var.network
  }

  env = concat(local.env, local.gen_env)

  log_driver = var.log_driver.name

  log_opts = {
    fluentd-address = var.log_driver.address
  }

  ports {
    internal = 1080
    external = var.exposed_port
  }
}
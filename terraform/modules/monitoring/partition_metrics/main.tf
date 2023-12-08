resource "docker_image" "partition_metrics" {
  name         = "${var.image}:${var.tag}"
  keep_locally = true
}

resource "docker_container" "partition_metrics" {
  name  = "armonik.control.partition_metrics"
  image = docker_image.partition_metrics.image_id

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
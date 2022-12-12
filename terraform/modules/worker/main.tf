resource "docker_image" "worker" {
  name         = "${var.docker_image}:${var.core_tag}"
  keep_locally = true
}

resource "docker_container" "worker" {
  name  = var.container_name
  image = docker_image.worker.image_id

  networks_advanced {
    name = var.network
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

  ports {
    internal = 1080
    external = 1080 + var.replica_counter
  }

  mounts {
    type   = "volume"
    target = "/cache"
    source = var.socket_vol
  }
}
resource "docker_image" "submitter" {
  name         = "${var.docker_image}:${var.core_tag}"
  keep_locally = true
}

resource "docker_container" "submitter" {
  name  = var.container_name
  image = docker_image.submitter.image_id

  networks_advanced {
    name = var.network
  }

  env = concat(local.env, local.gen_env)

  log_driver = var.log_driver.name
  log_opts   = var.log_driver.log_opts

  ports {
    internal = 1080
    external = 5001
  }

  ports {
    internal = 1081
    external = 5011
  }

  dynamic "mounts" {
    for_each = var.volumes
    content {
      type   = "volume"
      target = mounts.value
      source = mounts.key
    }
  }

  wait = true
  healthcheck {
    test         = ["CMD", "/healthchecker/ArmoniK.Core.HealthChecker.exe", "http://localhost:1081/liveness"]
    interval     = "5s"
    timeout      = "3s"
    start_period = "20s"
    retries      = 5
  }
}

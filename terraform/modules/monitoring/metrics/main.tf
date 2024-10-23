resource "docker_image" "metrics" {
  name         = "${var.image}:${var.tag}"
  keep_locally = true
}

resource "docker_container" "metrics" {
  name  = "armonik.control.metrics"
  image = docker_image.metrics.image_id

  networks_advanced {
    name = var.network
  }

  env = local.gen_env

  log_driver = var.log_driver.name
  log_opts   = var.log_driver.log_opts

  ports {
    internal = 1080
    external = var.exposed_port
  }

  wait = true
  healthcheck {
    test         = ["CMD", "/healthchecker/ArmoniK.Core.HealthChecker.exe"]
    interval     = "5s"
    timeout      = "3s"
    start_period = "20s"
    retries      = 5
  }
}

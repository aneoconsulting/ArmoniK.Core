resource "docker_image" "queue" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "queue" {
  name  = "queue"
  image = docker_image.queue.image_id

  networks_advanced {
    name = var.network.name
  }
  network_mode = var.network.driver

  env  = ["BROKER_LOG=${var.log_level}"]
  wait = !var.windows

  ports {
    internal = 8080
    external = var.exposed_ports.connection
  }

  dynamic "healthcheck" {
    for_each = var.windows ? [] : [1]
    content {
      test         = ["CMD", "/usr/local/bin/armonik-broker", "health"]
      interval     = "5s"
      timeout      = "3s"
      start_period = "5s"
      retries      = "10"
    }
  }
}

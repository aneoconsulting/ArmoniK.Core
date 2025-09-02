resource "docker_image" "queue" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "queue" {
  name  = "queue"
  image = docker_image.queue.image_id

  networks_advanced {
    name = var.network
  }

  command = ["-js", "--http_port", "8222"]
  wait    = !var.windows

  ports {
    internal = 4222
    external = var.exposed_ports.connection
  }

  dynamic "healthcheck" {
    for_each = var.windows ? [] : [1]
    content {
      test         = concat(["CMD", "wget", "http://localhost:8222/healthz"])
      interval     = "10s"
      timeout      = "3s"
      start_period = "10s"
      retries      = "10"
    }
  }
}
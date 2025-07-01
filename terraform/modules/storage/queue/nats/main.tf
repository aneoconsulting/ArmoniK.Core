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

  command = ["-js"]
  wait    = true

  ports {
    internal = 8085
    external = var.exposed_ports.connection
  }

  healthcheck {
    test         = concat(["CMD", "curl", "-fsSl", "localhost:8085"])
    interval     = "10s"
    timeout      = "3s"
    start_period = "10s"
    retries      = "10"
  }
}

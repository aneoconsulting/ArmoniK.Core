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

  wait = true

  ports {
    internal = 9324
    external = var.exposed_ports.connection
  }

  healthcheck {
    test         = concat(["CMD", "curl", "-fsSl", "http://localhost:9324/?Action=ListQueues&Version=2012-11-05"])
    interval     = "10s"
    timeout      = "3s"
    start_period = "10s"
    retries      = "10"
  }
}

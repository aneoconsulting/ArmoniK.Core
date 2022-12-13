resource "docker_image" "queue" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "queue" {
  name  = "queue"
  image = docker_image.queue.image_id

  command = ["sh -c echo ${var.plugins}. > /etc/rabbitmq/enabled_plugins && exec docker-entrypoint.sh rabbitmq-server"]

  networks_advanced {
    name = var.network
  }

  ports {
    internal = 5672
    external = var.exposed_ports[0]
    ip       = "127.0.0.1"
  }

  ports {
    internal = 15672
    external = var.exposed_ports[1]
  }

  healthcheck {
    test         = ["rabbitmq-diagnostics", "check_port_connectivity"]
    interval     = "10s"
    timeout      = "5s"
    start_period = "10s"
    retries      = "10"
  }
}
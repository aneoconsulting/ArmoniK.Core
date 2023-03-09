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

  env = [
    "AMQ_EXTRA_ARGS=--relax-jolokia --http-host 0.0.0.0",
    "AMQ_USER=${var.queue_envs.user}",
    "AMQ_PASSWORD=${var.queue_envs.password}",
    "AMQ_HOME=/opt/amq",
    "AMQ_ROLE=admin"
  ]

  ports {
    internal = 5672
    external = var.exposed_ports.amqp_connector
  }

  ports {
    internal = 8161
    external = var.exposed_ports.admin_interface
  }
}
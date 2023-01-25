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

  env = ["ENABLE_JMX_EXPORTER=true",
    "ARTEMIS_MIN_MEMORY=1512M",
    "ARTEMIS_MAX_MEMORY=2000M",
    "ARTEMIS_USERNAME=guest",
  "ARTEMIS_PASSWORD=guest"]

  ports {
    internal = 5672
    external = var.exposed_ports.amqp_connector
  }

  ports {
    internal = 8161
    external = var.exposed_ports.admin_interface
  }
}
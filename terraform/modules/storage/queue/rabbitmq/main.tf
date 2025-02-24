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

  ports {
    internal = 5672
    external = var.exposed_ports.amqp_connector
  }

  ports {
    internal = 15672
    external = var.exposed_ports.admin_interface
  }

  upload {
    file    = "/etc/rabbitmq/enabled_plugins"
    content = "[rabbitmq_management ,rabbitmq_management_agent ${local.plug}]."
  }

  upload {
    file   = "/etc/rabbitmq/conf.d/10-defaults.conf"
    source = abspath("${path.root}/rabbitmq/rabbitmq.conf")
  }
}

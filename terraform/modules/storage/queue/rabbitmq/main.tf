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
    internal = local.is_windows ? 5672 : 5671
    external = var.exposed_ports.amqp_connector
  }

  ports {
    internal = 15671
    external = var.exposed_ports.admin_interface
  }
  dynamic "upload" {
    for_each = local.is_windows ? [] : [
      { file = "/rabbitmq/certs/rabbit.key", content = local_file.key.content },
      { file = "/rabbitmq/certs/rabbit.crt", content = local_file.cert.content },
      { file = "/rabbitmq/certs/ca.pem", content = local_file.ca.content }
    ]

    content {
      file    = upload.value.file
      content = upload.value.content
    }
  }

  upload {
    file    = "/etc/rabbitmq/enabled_plugins"
    content = "[rabbitmq_management ,rabbitmq_management_agent ${local.plug}]."
  }

  upload {
    file   = "/etc/rabbitmq/conf.d/10-defaults.conf"
    source = abspath("${path.root}/rabbitmq/rabbitmq.conf")
  }
  healthcheck {
    test         = ["CMD-SHELL", "rabbitmq-diagnostics status "]
    interval     = "10s"
    timeout      = "10s"
    retries      = 10
    start_period = "30s"
  }
}
resource "time_sleep" "wait_for_rabbit" {
  depends_on      = [docker_container.queue]
  create_duration = "10s"
}
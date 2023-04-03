resource "docker_image" "queue" {
  name         = var.image
  keep_locally = true
}

resource "local_file" "plugins" {
  content  = "[rabbitmq_management ,rabbitmq_management_agent ${local.plug}]."
  filename = "${path.module}/plugins"
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
    ip       = "127.0.0.1"
  }

  ports {
    internal = 15672
    external = var.exposed_ports.admin_interface
  }

  wait         = true
  wait_timeout = 30

  healthcheck {
    test         = ["CMD", "rabbitmq-diagnostics", "check_port_connectivity"]
    interval     = "10s"
    timeout      = "5s"
    start_period = "10s"
    retries      = "10"
  }

  mounts {
    type   = "bind"
    target = "/etc/rabbitmq/enabled_plugins"
    source = abspath("${local_file.plugins.filename}")
  }
}
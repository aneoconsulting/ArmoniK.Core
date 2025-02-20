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
    "Components__QueueAdaptorSettings__ClassName=${"ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder"}",
    "Components__QueueAdaptorSettings__AdapterAbsolutePath=${path.root}/Adaptors/RabbitMQ/src/bin/Debug/net8.0/ArmoniK.Core.Adapters.RabbitMQ.dll",
    "Amqp__User=guest",
    "Amqp__Password=guest",
    "Amqp__Host=localhost",
    "Amqp__Port=${var.queue_envs.port}",
    "Amqp__Scheme=amqp",
    "Amqp__PartitionId=TestPartition",
    "Amqp__MaxPriority=${var.queue_envs.max_priority}",
    "Amqp__MaxRetries=${var.queue_envs.max_retries}",
    "Amqp__LinkCredit=${var.queue_envs.link_credit}",
    "Amqp__AllowHostMismatch=false"
  ]
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

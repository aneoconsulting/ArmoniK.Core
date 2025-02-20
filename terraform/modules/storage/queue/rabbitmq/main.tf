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
  # env = [
  #   "Components__QueueAdaptorSettings__ClassName=${local.generated_env_vars.Components__QueueAdaptorSettings__ClassName}",
  #   "Components__QueueAdaptorSettings__AdapterAbsolutePath=${path.root}/Adaptors/RabbitMQ/src/bin/Debug/net8.0/ArmoniK.Core.Adapters.RabbitMQ.dll",
  #   "Amqp__User=${local.generated_env_vars.Amqp__User}",
  #   "Amqp__Password=${local.generated_env_vars.Amqp__Password}",
  #   "Amqp__Host=${local.generated_env_vars.Amqp__Host}",
  #   "Amqp__Port=${local.generated_env_vars.Amqp__Port}",
  #   "Amqp__Scheme=${local.generated_env_vars.Amqp__Scheme}",
  #   "Amqp__PartitionId=${local.generated_env_vars.Amqp__PartitionId}",
  #   "Amqp__MaxPriority=${local.generated_env_vars.Amqp__MaxPriority}",
  #   "Amqp__MaxRetries=${local.generated_env_vars.Amqp__MaxRetries}",
  #   "Amqp__LinkCredit=${local.generated_env_vars.Amqp__LinkCredit}",
  #   "Amqp__AllowHostMismatch=false"
  # ]
  env = local.generated_env
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

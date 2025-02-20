locals {
  plug = var.protocol == "amqp1_0" ? ",rabbitmq_amqp1_0" : ""
  generated_env_vars = {
    Components__QueueAdaptorSettings__ClassName           = var.protocol == "amqp1_0" ? "ArmoniK.Core.Adapters.Amqp.QueueBuilder" : "ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder"
    Components__QueueAdaptorSettings__AdapterAbsolutePath = var.protocol == "amqp1_0" ? "/adapters/queue/amqp/ArmoniK.Core.Adapters.Amqp.dll" : "/adapters/queue/rabbit/ArmoniK.Core.Adapters.RabbitMQ.dll"
    Amqp__User                                            = "guest"
    Amqp__Password                                        = "guest"
    Amqp__Host                                            = var.queue_envs.host
    Amqp__Port                                            = var.queue_envs.port
    Amqp__Scheme                                          = "AMQP"
    Amqp__PartitionId                                     = "TestPartition"
    Amqp__MaxPriority                                     = var.queue_envs.max_priority
    Amqp__MaxRetries                                      = var.queue_envs.max_retries
    Amqp__LinkCredit                                      = var.queue_envs.link_credit
  }
  generated_env = [
    "Components__QueueAdaptorSettings__ClassName=ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder",
    "Components__QueueAdaptorSettings__AdapterAbsolutePath=${path.root}/Adaptors/RabbitMQ/src/bin/Debug/net8.0/ArmoniK.Core.Adapters.RabbitMQ.dll",
    "Amqp__User=${local.generated_env_vars.Amqp__User}",
    "Amqp__Password=${local.generated_env_vars.Amqp__Password}",
    "Amqp__Host=localhost",
    "Amqp__Port=${local.generated_env_vars.Amqp__Port}",
    "Amqp__Scheme=${local.generated_env_vars.Amqp__Scheme}",
    "Amqp__PartitionId=${local.generated_env_vars.Amqp__PartitionId}",
    "Amqp__MaxPriority=${local.generated_env_vars.Amqp__MaxPriority}",
    "Amqp__MaxRetries=${local.generated_env_vars.Amqp__MaxRetries}",
    "Amqp__LinkCredit=${local.generated_env_vars.Amqp__LinkCredit}",
    "Amqp__AllowHostMismatch=false"
  ]
}
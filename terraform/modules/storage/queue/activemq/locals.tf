locals {
  generated_env_vars = {
    Components__QueueAdaptorSettings__ClassName           = "ArmoniK.Core.Adapters.Amqp.QueueBuilder"
    Components__QueueAdaptorSettings__AdapterAbsolutePath = "/adapters/queue/amqp/ArmoniK.Core.Adapters.Amqp.dll"
    Amqp__User                                            = var.queue_envs.user
    Amqp__Password                                        = var.queue_envs.password
    Amqp__Host                                            = var.queue_envs.host
    Amqp__Port                                            = var.queue_envs.port
    Amqp__Scheme                                          = "AMQP"
    Amqp__PartitionId                                     = "TestPartition"
    Amqp__MaxPriority                                     = var.queue_envs.max_priority
    Amqp__MaxRetries                                      = var.queue_envs.max_retries
    Amqp__LinkCredit                                      = var.queue_envs.link_credit
  }

  generated_env = [
    "Components__QueueAdaptorSettings__ClassName=${local.generated_env_vars.Components__QueueAdaptorSettings__ClassName}",
    "Components__QueueAdaptorSettings__AdapterAbsolutePath=${path.root}/Adaptors/Amqp/src/bin/Debug/net8.0/ArmoniK.Core.Adapters.Amqp.dll",
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

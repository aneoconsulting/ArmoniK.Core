output "generated_env_vars" {
  value = ({
    "Components__QueueAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.Nats.QueueBuilder"
    "Components__QueueAdaptorSettings__AdapterAbsolutePath" = "/adapters/queue/nats/ArmoniK.Core.Adapters.Nats.dll"
    "Nats__Url"                                             = "${var.queue_envs.host}:4222"
    "Nats__MaxPriority"                                     = "9"
    "Nats__WaitTimeSeconds"                                 = "00:00:00"
  })
}
output "core_mounts" {
  description = "Volumes that agents and submitters must mount to access the queue"
  value = {
  }
}
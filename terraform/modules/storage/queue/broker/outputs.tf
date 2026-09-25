output "generated_env_vars" {
  value = ({
    "Components__QueueAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.Broker.QueueBuilder"
    "Components__QueueAdaptorSettings__AdapterAbsolutePath" = "/adapters/queue/broker/ArmoniK.Core.Adapters.Broker.dll"
    "Broker__Endpoint"                                   = "http://${var.queue_envs.host}:8080"
    "Broker__Affinity"                                   = tostring(var.affinity)
  })
}
output "core_mounts" {
  description = "Volumes that agents and submitters must mount to access the queue"
  value = {
  }
}

output "generated_env_vars" {
  value = ({
    "Components__QueueAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.Nats.QueueBuilder"
    "Components__QueueAdaptorSettings__AdapterAbsolutePath" = "/adapters/queue/nats/ArmoniK.Core.Adapters.Nats.dll"
    "PubSub__ProjectId"                                     = "plugincore"
    "PubSub__TopicId"                                       = "TestTopic"
    "PubSub__SubscriptionId"                                = "SubTest"
    "PUBSUB_EMULATOR_HOST"                                  = "${var.queue_envs.host}:8085"
  })
}
output "core_mounts" {
  description = "Volumes that agents and submitters must mount to access the queue"
  value = {
  }
}
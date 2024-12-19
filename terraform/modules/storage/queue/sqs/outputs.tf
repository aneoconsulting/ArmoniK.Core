output "generated_env_vars" {
  value = ({
    "Components__QueueAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.SQS.QueueBuilder"
    "Components__QueueAdaptorSettings__AdapterAbsolutePath" = "/adapters/queue/sqs/ArmoniK.Core.Adapters.SQS.dll"
    "SQS__ServiceURL"                                       = "http://${var.queue_envs.host}:4566"
    "SQS__PartitionId"                                      = "TestPartition0"
    "SQS__Prefix"                                           = "armonik"
    "SQS__Tags__deployment"                                 = "docker"
    "AWS_ACCESS_KEY_ID"                                     = "localkey"
    "AWS_SECRET_ACCESS_KEY"                                 = "localsecret"
  })
}

output "generated_env_vars" {
  value = ({
    "Components__QueueAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.Amqp.QueueBuilder"
    "Components__QueueAdaptorSettings__AdapterAbsolutePath" = "/adapters/queue/amqp/ArmoniK.Core.Adapters.Amqp.dll"
    "Amqp__User"                                            = "${var.queue_envs.user}"
    "Amqp__Password"                                        = "${var.queue_envs.password}"
    "Amqp__Host"                                            = "${var.queue_envs.host}"
    "Amqp__Port"                                            = "${var.queue_envs.port}"
    "Amqp__Ssl"                                             = true
    "Amqp__Scheme"                                          = "AMQPS"
    "Amqp__CaPath"                                          = "/activemq/certs/ca.pem"
    "Amqp__CertPath"                                        = "/activemq/certs/rabbit.crt"
    "Amqp__KeyPath"                                         = "/activemq/certs/rabbit.key"
    "Amqp__Timeout"                                         = "20000"
    "Amqp__MaxPriority"                                     = "${var.queue_envs.max_priority}"
    "Amqp__MaxRetries"                                      = "${var.queue_envs.max_retries}"
    "Amqp__LinkCredit"                                      = "${var.queue_envs.link_credit}"
  })
}
output "core_mounts" {
  description = "Volumes that agents and submitters must mount to access the queue"
  value = {

  }
}
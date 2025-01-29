output "generated_env_vars" {
  value = ({
    # RabbitMQ adapter targets version 0.9.1 of the protocol, if the version 1.0.0 is to be used the AMQP adapter should be employed
    "Components__QueueAdaptorSettings__ClassName"           = var.protocol == "amqp1_0" ? "ArmoniK.Core.Adapters.Amqp.QueueBuilder" : "ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder"
    "Components__QueueAdaptorSettings__AdapterAbsolutePath" = var.protocol == "amqp1_0" ? "/adapters/queue/amqp/ArmoniK.Core.Adapters.Amqp.dll" : "/adapters/queue/rabbit/ArmoniK.Core.Adapters.RabbitMQ.dll"
    "Amqp__User"                                            = "guest" # Default value, to change it we should provide a suitable .conf file
    "Amqp__Password"                                        = "guest" # Default value, to change it we should provide a suitable .conf file
    "Amqp__Ssl"                                             = true
    "Amqp__Host"                                            = "${var.queue_envs.host}"
    "Amqp__Port"                                            = "${var.queue_envs.port}"
    "Amqp__Scheme"                                          = "AMQPS"
    "Amqp__CaPath"                                          = "/rabbitmq/certs/ca.pem"
    "Amqp__CertPath"                                        = "/rabbitmq/certs/rabbit.crt"
    "Amqp__KeyPath"                                         = "/rabbitmq/certs/rabbit.key"
    "Amqp__Timeout"                                         = "20000"
    "Amqp__MaxPriority"                                     = "${var.queue_envs.max_priority}"
    "Amqp__MaxRetries"                                      = "${var.queue_envs.max_retries}"
    "Amqp__LinkCredit"                                      = "${var.queue_envs.link_credit}"
  })
}

output "core_mounts" {
  description = "Volumes that agents and submitters must mount to access the queue"
  value = {
    "/rabbitmq/certs/ca.pem" = local_file.ca.filename
  }
}
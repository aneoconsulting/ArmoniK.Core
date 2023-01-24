output "generated_env_vars" {
  value = ({
    # RabbitMQ adapter targets version 0.9.1 of the protocol, if the version 1.0.0 is to be used the AMQP adapter should be employed
    "Components__QueueStorage" = var.protocol == "amqp1_0" ? "ArmoniK.Adapters.Amqp.QueueStorage" : "ArmoniK.Adapters.RabbitMQ.QueueStorage"
    "Amqp__User"               = "guest" # Default value, to change it we should provide a suitable .conf file
    "Amqp__Password"           = "guest" # Default value, to change it we should provide a suitable .conf file
    "Amqp__Host"               = "${var.queue_envs.host}"
    "Amqp__Port"               = "${var.queue_envs.port}"
    "Amqp__Scheme"             = "AMQP"
    "Amqp__MaxPriority"        = "${var.queue_envs.max_priority}"
    "Amqp__MaxRetries"         = "${var.queue_envs.max_retries}"
    "Amqp__LinkCredit"         = "${var.queue_envs.link_credit}"
  })
}
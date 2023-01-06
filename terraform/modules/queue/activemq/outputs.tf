output "queue_entry_points" { #TODO: to be removed
  value = var.exposed_ports
}

output "queue_env_vars" {
  value = ({
    "Components__QueueStorage" = "ArmoniK.Adapters.Amqp.QueueStorage"
    "Amqp__User"               = "${var.queue_storage.user}"
    "Amqp__Password"           = "${var.queue_storage.password}"
    "Amqp__Host"               = "${var.queue_storage.host}"
    "Amqp__Port"               = "${var.queue_storage.port}"
    "Amqp__Scheme"             = "AMQP"
    "Amqp__MaxPriority"        = "${var.queue_storage.max_priority}"
    "Amqp__MaxRetries"         = "${var.queue_storage.max_retries}"
    "Amqp__LinkCredit"         = "${var.queue_storage.link_credit}"
  })
}
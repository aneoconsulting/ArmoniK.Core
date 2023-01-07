variable "core_tag" {
  type    = string
  default = "test"
}

variable "use_local_image" {
  type    = bool
  default = false
}

variable "num_replicas" {
  type    = number
  default = 3
}

variable "mongodb_params" {
  type = object({
    max_connection_pool_size = string
    min_polling_delay        = string
    max_polling_delay        = string
  })
}

variable "submitter" {
  type = object({
    name            = string,
    image           = string,
    port            = number,
    log_level       = string,
    aspnet_core_env = string,
  })
}

variable "queue_storage" {
  type = object({
    protocol = string,
    broker = object({
      name  = string
      image = string
    })
    user         = string,
    password     = string,
    host         = string,
    port         = number,
    max_priority = number,
    max_retries  = number,
    link_credit  = number,
    partition    = string
  })
  description = "Parameters to define the broker, protocol and queue settings"
  validation {
    condition     = can(regex("^(amqp1_0|amqp0_9_1)$", var.queue_storage.protocol))
    error_message = "Protocol must be amqp1_0|amqp0_9_1"
  }
  validation {
    condition     = can(regex("^(activemq|rabbitmq)$", var.queue_storage.broker.name))
    error_message = "Must be activemq or rabbitmq"
  }
}

variable "compute_plane" {
  type = object({
    log_level       = string,
    aspnet_core_env = string,

    worker = object({
      name                     = string,
      image                    = string,
      port                     = number,
      serilog_application_name = string
    })

    polling_agent = object({
      name                 = string,
      image                = string,
      port                 = number,
      max_error_allowed    = number,
      worker_check_retries = number,
      worker_check_delay   = string,
    })
  })
}

variable "armonik_metrics_image" {
  type = string
}

variable "armonik_partition_metrics_image" {
  type = string
}

variable "log_driver_image" {
  type = string
}

variable "seq_image" {
  type = string
}

variable "zipkin_image" {
  type = string
}

variable "database_image" {
  type = string
}

variable "object_image" {
  type = string
}

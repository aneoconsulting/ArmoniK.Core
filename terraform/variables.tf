variable "core_tag" {
  type    = string
  default = "test"
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
    object_storage  = string,
  })
}

variable "queue_storage" {
  type = object({
    user         = string,
    password     = string,
    host         = string,
    port         = number,
    max_priority = number,
    max_retries  = number,
    link_credit  = number,
    partition    = string
  })
}

variable "compute_plane" {
  type = object({
    log_level       = string,
    aspnet_core_env = string,
    object_storage  = string,

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
  type    = string
  default = "dockerhubaneo/armonik_control_metrics"
}

variable "armonik_partition_metrics_image" {
  type    = string
  default = "dockerhubaneo/armonik_control_partition_metrics"
}

variable "use_local_image" {
  type    = bool
  default = false
}

variable "num_replicas" {
  type    = number
  default = 3
}

variable "log_driver" {
  type    = string
  default = "fluent/fluent-bit:latest"
}

variable "seq_image" {
  type    = string
  default = "datalust/seq:latest"
}

variable "zipkin_image" {
  type    = string
  default = "openzipkin/zipkin:latest"
}

variable "database_image" {
  type    = string
  default = "mongo"
}

variable "object_image" {
  type    = string
  default = "redis:bullseye"
}

variable "queue_image" {
  type    = string
  default = "symptoma/activemq:5.16.3"
}


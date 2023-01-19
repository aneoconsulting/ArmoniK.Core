variable "core_tag" {
  type    = string
  default = "0.8.0"
}

variable "use_local_image" {
  type    = bool
  default = false
}

variable "num_replicas" {
  type    = number
  default = 3
}

variable "num_partitions" {
  type    = number
  default = 3
}

variable "mongodb_params" {
  type = object({
    max_connection_pool_size = string
    min_polling_delay        = string
    max_polling_delay        = string
  })
  default = {
    max_connection_pool_size = "500"
    min_polling_delay        = "00:00:01"
    max_polling_delay        = "00:00:10"
  }
}

variable "logging_env_vars" {
  type = object({
    log_level       = string,
    aspnet_core_env = string,
  })
  default = {
    aspnet_core_env = "Development"
    log_level       = "Information"
  }
}

variable "submitter" {
  type = object({
    name  = string,
    image = string,
    port  = number,
  })
  default = {
    image = "dockerhubaneo/armonik_control"
    name  = "armonik.control.submitter"
    port  = 5001
  }
}

variable "object_storage" {
  type = object({
    name  = string
    image = string
  })
  validation {
    condition     = can(regex("^(redis|local)$", var.object_storage.name))
    error_message = "Must be redis or local"
  }
  default = {
    image = ""
    name  = "local"
  }
}

variable "queue_storage" {
  type = object({
    protocol = string, # Only relevant for the RabbitMQ module
    broker = object({
      name  = string
      image = string
    })
    envs = object({
      user         = string,
      password     = string,
      host         = string,
      port         = number,
      max_priority = number,
      max_retries  = number,
      link_credit  = number,
      partition    = string
    })
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
  default = {
    protocol = "amqp1_0"
    broker = {
      name  = "rabbitmq"
      image = "rabbitmq:3-management"
    }
    envs = {
      host         = "queue"
      link_credit  = 2
      max_priority = 10
      max_retries  = 10
      partition    = "TestPartition"
      password     = "admin"
      port         = 5672
      user         = "admin"
    }
  }
}

variable "compute_plane" {
  type = object({
    worker = object({
      name                     = string,
      image                    = string,
      port                     = number,
      docker_file_path         = string,
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
  default = {
    polling_agent = {
      image                = "dockerhubaneo/armonik_pollingagent"
      max_error_allowed    = -1
      name                 = "armonik.compute.pollingagent"
      port                 = 9980
      worker_check_delay   = "00:00:10"
      worker_check_retries = 10
    }
    worker = {
      image                    = "dockerhubaneo/armonik_core_htcmock_test_worker"
      name                     = "armonik.compute.worker"
      port                     = 1080
      serilog_application_name = "ArmoniK.Compute.Worker"
      docker_file_path         = "../Tests/HtcMock/Server/src"
    }
  }
}

variable "partition_data" {
  description = "Template to create multiple partitions"
  type = object({
    _id                   = string
    priority              = number
    reserved_pods         = number
    max_pods              = number
    preemption_percentage = number
    parent_partition_ids  = string
    pod_configuration     = string
  })
  default = {
    _id                   = "TestPartition"
    priority              = 1
    reserved_pods         = 50
    max_pods              = 100
    preemption_percentage = 20
    parent_partition_ids  = "[]"
    pod_configuration     = "null"
  }
}

variable "armonik_metrics_image" {
  type    = string
  default = "dockerhubaneo/armonik_control_metrics"
}

variable "armonik_partition_metrics_image" {
  type    = string
  default = "dockerhubaneo/armonik_control_partition_metrics"
}

variable "log_driver_image" {
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

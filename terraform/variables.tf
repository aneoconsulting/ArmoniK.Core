variable "core_tag" {
  type    = string
  default = "test"
}

variable "use_local_image" {
  type    = bool
  default = false
}

variable "num_replicas" {
  type    = string
  default = "3"
}

variable "num_partitions" {
  type    = string
  default = "3"
}

variable "mongodb_params" {
  type = object({
    max_connection_pool_size = optional(string, "500")
    min_polling_delay        = optional(string, "00:00:01")
    max_polling_delay        = optional(string, "00:00:10")
    replica_set_name         = optional(string, "repSet0")
    use_direct_connection    = optional(bool, true)
    database_name            = optional(string, "database")
    exposed_port             = optional(number, 27017)
  })
  default = {}
}

variable "serilog" {
  type = object({
    loggin_level         = optional(string, "Information")
    loggin_level_routing = optional(string, "Warning")
  })
  default = {}
}

variable "aspnet_core_env" {
  type    = string
  default = "Development"
}

variable "submitter" {
  type = object({
    name  = optional(string, "armonik.control.submitter")
    image = optional(string, "dockerhubaneo/armonik_control")
    port  = optional(number, 5001)
  })
  default = {}
}

variable "object_storage" {
  type = object({
    name  = optional(string, "local")
    image = optional(string, "")
    # used by minio :
    host        = optional(string, "minio")
    port        = optional(number, 9000)
    login       = optional(string, "minioadmin")
    password    = optional(string, "minioadmin")
    bucket_name = optional(string, "miniobucket")

  })
  validation {
    condition     = can(regex("^(redis|local|minio)$", var.object_storage.name))
    error_message = "Must be redis, minio, or local"
  }
  default = {}
}

variable "queue_storage" {
  type = object({
    protocol = optional(string, "amqp1_0")
    name     = optional(string, "rabbitmq")
    image    = optional(string, "rabbitmq:3-management")
  })
  description = "Parameters to define the broker and protocol"
  validation {
    condition     = can(regex("^(amqp1_0|amqp0_9_1)$", var.queue_storage.protocol))
    error_message = "Protocol must be amqp1_0|amqp0_9_1"
  }
  validation {
    condition     = can(regex("^(activemq|rabbitmq|artemis)$", var.queue_storage.name))
    error_message = "Must be activemq, rabbitmq or artemis"
  }
  default = {}
}

variable "queue_env_vars" {
  type = object({
    user         = optional(string, "admin"),
    password     = optional(string, "admin"),
    host         = optional(string, "queue")
    port         = optional(number, 5672)
    max_priority = optional(number, 10)
    max_retries  = optional(number, 10)
    link_credit  = optional(number, 2)
    partition    = optional(string, "TestPartition")
  })
  description = "Environment variables for the queue"
  default     = {}
}

variable "worker_image" {
  type    = string
  default = "dockerhubaneo/armonik_core_htcmock_test_worker"
}

variable "worker_docker_file_path" {
  type    = string
  default = "../Tests/HtcMock/Server/src/"
}

variable "compute_plane" {
  type = object({
    worker = object({
      name                     = optional(string, "armonik.compute.worker")
      port                     = optional(number, 1080)
      serilog_application_name = optional(string, "ArmoniK.Compute.Worker")
    })

    polling_agent = object({
      name                 = optional(string, "armonik.compute.pollingagent")
      image                = optional(string, "dockerhubaneo/armonik_pollingagent")
      port                 = optional(number, 9980)
      max_error_allowed    = optional(number, -1)
      worker_check_retries = optional(number, 10)
      worker_check_delay   = optional(string, "00:00:01")
    })
  })
  default = {
    polling_agent = {}
    worker        = {}
  }
}

variable "partition_data" {
  description = "Template to create multiple partitions"
  type = object({
    _id                  = optional(string, "TestPartition")
    Priority             = optional(number, 1)
    PodReserved          = optional(number, 50)
    PodMax               = optional(number, 100)
    PreemptionPercentage = optional(number, 20)
    ParentPartitionIds   = optional(list(string), [])
    PodConfiguration = optional(object({
      Configuration = map(string)
    }), null)
  })
  default = {}
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

variable "grafana_image" {
  type    = string
  default = "grafana/grafana:latest"
}

variable "prometheus_image" {
  type    = string
  default = "prom/prometheus:latest"
}

variable "database_image" {
  type    = string
  default = "mongo"
}

variable "ingress" {
  type = object({
    image = optional(string, "nginxinc/nginx-unprivileged"),
    tag   = optional(string, "1.23.3"),
    configs = map(object({
      port = number,
      tls  = optional(bool, false),
      mtls = optional(bool, false),
  })) })
  default = {
    configs = {
      ingress = {
        port = 5201
      },
      ingress_tls = {
        port = 5202,
        tls  = true
      },
      ingress_mtls = {
        port = 5203,
        tls  = true,
        mtls = true
      }
  } }
}
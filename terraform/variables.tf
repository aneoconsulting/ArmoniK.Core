variable "core-tag" {
  type    = string
  default = "test"
}

variable "armonik-submitter-image" {
  type    = string
  default = "dockerhubaneo/armonik_control"
}

variable "armonik-metrics-image" {
  type    = string
  default = "dockerhubaneo/armonik_control_metrics"
}

variable "armonik-partition-metrics-image" {
  type    = string
  default = "dockerhubaneo/armonik_control_partition_metrics"
}

variable "armonik-pollingagent-image" {
  type    = string
  default = "dockerhubaneo/armonik_pollingagent"
}

variable "armonik-worker-image" {
  type    = string
  default = "dockerhubaneo/armonik_core_htcmock_test_worker"
}

variable "log-driver" {
  type    = string
  default = "fluent/fluent-bit:latest"
}

variable "seq-image" {
  type    = string
  default = "datalust/seq:latest"
}

variable "zipkin-image" {
  type    = string
  default = "openzipkin/zipkin:latest"
}

variable "database-image" {
  type    = string
  default = "mongo"
}

variable "object-image" {
  type    = string
  default = "redis:bullseye"
}

variable "queue-image" {
  type    = string
  default = "symptoma/activemq:5.16.3"
}


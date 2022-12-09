variable "core_tag" {
  type    = string
  default = "test"
}

variable "armonik_submitter_image" {
  type    = string
  default = "dockerhubaneo/armonik_control"
}

variable "armonik_metrics_image" {
  type    = string
  default = "dockerhubaneo/armonik_control_metrics"
}

variable "armonik_partition_metrics_image" {
  type    = string
  default = "dockerhubaneo/armonik_control_partition_metrics"
}

variable "armonik_pollingagent_image" {
  type    = string
  default = "dockerhubaneo/armonik_pollingagent"
}

variable "armonik_worker_image" {
  type    = string
  default = "dockerhubaneo/armonik_core_htcmock_test_worker"
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


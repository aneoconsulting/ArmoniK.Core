variable "otel_collector_image" {
  type = string
}

variable "zipkin_image" {
  type = string
}

variable "network" {
  type = string
}

variable "ingestion_ports" {
  type = object({
    http   = number
    zipkin = number
  })
}

variable "exporters" {
  type = object({
    file   = bool
    zipkin = bool
  })
}
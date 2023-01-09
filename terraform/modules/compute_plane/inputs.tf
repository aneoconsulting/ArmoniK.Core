variable "core_tag" {
  type = string
}

variable "dev_env" {
  type = string
}

variable "log_level" {
  type = string
}

variable "polling_agent" {
  type = object({
    name                 = string,
    image                = string,
    port                 = number,
    max_error_allowed    = number,
    worker_check_retries = number,
    worker_check_delay   = string,
  })
}

variable "worker" {
  type = object({
    name                     = string,
    image                    = string,
    port                     = number,
    serilog_application_name = string
  })
}

variable "queue_env_vars" {
  type = map(string)
}

variable "database_env_vars" {
  type = map(string)
}

variable "object_env_vars" {
  type = map(string)
}

variable "replica_counter" {
  type = number
}

variable "use_local_image" {
  type    = bool
  default = false
}

variable "network" {
  type = string
}

variable "zipkin_uri" {
  type = string
}

variable "log_driver" {
  type = object({
    name    = string,
    address = string,
  })
}
variable "core_tag" {
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
    shared_socket        = string
    shared_data          = string
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

variable "generated_env_vars" {
  type = map(string)
}

variable "volumes" {
  type = map(string)
}

variable "replica_counter" {
  type = number
}

variable "num_partitions" {
  type = number
}

variable "network" {
  type = string
}

variable "log_driver" {
  type = object({
    name     = string,
    log_opts = map(string),
  })
}

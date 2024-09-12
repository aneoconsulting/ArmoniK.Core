variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "queue_envs" {
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

variable "exposed_ports" {
  type = object({
    connection = number,
  })
  default = {
    connection = 4566
  }
}

variable "image" {
  type = string
}

variable "network" {
  type = object({
    name   = string
    driver = string
  })
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
  })
}

variable "queue_list" {
  type = list(string)
}

variable "exposed_ports" {
  type = object({
    connection = number,
  })
  default = {
    connection = 9324
  }
}

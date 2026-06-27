variable "image" {
  type = string
}

variable "network" {
  type = object({
    name   = string
    driver = string
  })
}

variable "windows" {
  type = bool
}

variable "postgresql_params" {
  type = object({
    user              = string
    password          = string
    database_name     = string
    ssl               = bool
    max_pool_size     = number
    max_connections   = number
    exposed_port      = number
    min_polling_delay = string
    max_polling_delay = string
  })
}

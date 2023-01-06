variable "tag" {
  type = string
}

variable "image" {
  type = string
}

variable "use_local_image" {
  type    = bool
  default = false
}

variable "network" {
  type = string
}

variable "exposed_port" {
  type    = number
  default = 5003
}

variable "mongodb_params" {
  type = object({
    max_connection_pool_size = string
    min_polling_delay        = string
    max_polling_delay        = string
  })
}

variable "db_driver" {
  type = object({
    name = string,
    port = number,
  })
}

variable "log_driver" {
  type = object({
    name    = string,
    address = string,
  })
}
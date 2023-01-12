variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "mongodb_params" {
  type = object({
    max_connection_pool_size = string
    min_polling_delay        = string
    max_polling_delay        = string
  })
}

variable "partition_list" {
  type = map(any)
}

variable "db_name" {
  type    = string
  default = "database"
}

variable "exposed_port" {
  type    = number
  default = 27017
}
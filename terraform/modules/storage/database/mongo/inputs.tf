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
    replica_set_name         = string
    use_direct_connection    = bool
    database_name            = string
    exposed_port             = number
  })
}

variable "partition_list" {
  type = map(any)
}

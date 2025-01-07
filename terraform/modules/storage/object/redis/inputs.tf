variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "exposed_port" {
  type = number
}

variable "redis_params" {
  type = object({
    host          = string
    port          = number
    user          = string
    password      = string
    tls_enabled   = bool
    Ssl           = bool
    ca_path       = string
    cert_path     = string
    key_path      = string
    credentials   = string
    database_name = string
    windows       = bool
  })
}

variable "object_storage" {
  type = object({
    name = string
  })
}
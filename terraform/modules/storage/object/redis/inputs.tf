variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "redis_params" {
  type = object({
    host          = string
    exposed_port  = number
    user          = string
    password      = string
    tls_enabled   = bool
    Ssl           = bool
    scheme        = string
    ca_path       = string
    cert_path     = string
    key_path      = string
    timeout       = number
    database_name = string
    windows       = bool
  })
}
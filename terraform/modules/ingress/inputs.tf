variable "container" {
  type = object({
    name  = string,
    image = string,
    tag   = string
  })
}

variable "network" {
  type = object({
    name   = string
    driver = string
  })
}

variable "port" {
  type = number
}

variable "tls" {
  type = bool
}

variable "mtls" {
  type = bool
}

variable "proxy" {
  description = "Authentication service consulted through nginx auth_request (oauth2-proxy), whose user and groups nginx writes for the Proxy method; null for none"
  type = object({
    url = string
  })
  default = null
}

variable "submitter" {
  type = object({
    url  = string,
    port = string,
    id   = string
  })
}

variable "log_driver" {
  type = object({
    name     = string,
    log_opts = map(string),
  })
}

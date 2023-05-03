variable "container" {
  type = object({
    name  = string,
    image = string,
    tag   = string
  })
}

variable "network" {
  type = string
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

variable "submitter" {
  type = object({
    url  = string,
    port = string,
    id   = string
  })
}

variable "log_driver" {
  type = object({
    name    = string,
    address = string,
  })
}
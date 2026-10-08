variable "image" {
  type = string
}

variable "network" {
  type = object({
    name   = string
    driver = string
  })
}

variable "exposed_port" {
  type    = number
  default = 8180
}

variable "log_driver" {
  type = object({
    name     = string,
    log_opts = map(string),
  })
}

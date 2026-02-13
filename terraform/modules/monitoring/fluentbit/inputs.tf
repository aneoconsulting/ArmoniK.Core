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
  default = 24224
}

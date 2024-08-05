variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "exposed_port" {
  type    = number
  default = 24224
}

variable "mask" {
  type    = string
  default = "127.0.0.1"
}

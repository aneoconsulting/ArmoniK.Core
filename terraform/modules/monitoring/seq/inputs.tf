variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "exposed_ports" {
  type    = list(number)
  default = [80, 5341]
}
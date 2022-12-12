variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "exposed_ports" {
  type    = list(number)
  default = [6572, 8161]
}
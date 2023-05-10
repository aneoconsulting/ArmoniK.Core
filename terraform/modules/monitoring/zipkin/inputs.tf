variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "exposed_port" {
  type    = number
  default = 9411
}
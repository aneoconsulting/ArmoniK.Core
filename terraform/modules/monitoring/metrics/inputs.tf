variable "tag" {
  type = string
}

variable "image" {
  type = string
}

variable "use_local_image" {
  type    = bool
  default = false
}

variable "network" {
  type = string
}

variable "exposed_port" {
  type    = number
  default = 5002
}

variable "log_driver_name" {
  type = string
}

variable "log_driver_address" {
  type = string
}
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

variable "db_driver" {
  type = object({
    name    = string,
    port    = number,
  })
}

variable "log_driver" {
  type = object({
    name    = string,
    address = string,
  })
}
variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "exposed_port" {
  type    = number
  default = 9000
}

variable "host" {
  type = string
}

variable "port" {
  type = number
}

variable "login" {
  type = string
}

variable "password" {
  type = string
}

variable "bucket_name" {
  type = string
}
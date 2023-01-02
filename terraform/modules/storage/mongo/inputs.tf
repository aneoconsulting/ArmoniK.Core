variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "db_name" {
  type = string
  default = "database"
}

variable "exposed_port" {
  type    = number
  default = 27017
}
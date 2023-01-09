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

variable "dev_env" {
  type = string
}

variable "log_level" {
  type = string
}

variable "database_env_vars" {
  type = map(string)
}

variable "exposed_port" {
  type    = number
  default = 5002
}

variable "log_driver" {
  type = object({
    name    = string,
    address = string,
  })
}
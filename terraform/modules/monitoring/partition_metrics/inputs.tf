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
  default = 5003
}

variable "dev_env" {
  type = string
}

variable "log_level" {
  type = string
}

variable "database_env_vars" {
  type = map(any)
}

variable "metrics_env_vars" {
  type = map(any)
}

variable "log_driver" {
  type = object({
    name    = string,
    address = string,
  })
}
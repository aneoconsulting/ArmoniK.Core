variable "tag" {
  type = string
}

variable "image" {
  type = string
}

variable "network" {
  type = object({
    name   = string
    driver = string
  })
}

variable "generated_env_vars" {
  type = map(string)
}

variable "mounts" {
  type = map(string)
}

variable "exposed_port" {
  type    = number
  default = 5002
}

variable "log_driver" {
  type = object({
    name     = string,
    log_opts = map(string),
  })
}

variable "container_init" {
  type = bool
}

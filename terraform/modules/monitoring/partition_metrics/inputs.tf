variable "tag" {
  type = string
}

variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "exposed_port" {
  type    = number
  default = 5003
}

variable "generated_env_vars" {
  type = map(string)
}

variable "metrics_env_vars" {
  type = map(string)
}

variable "log_driver" {
  type = object({
    name    = string,
    address = string,
  })
}
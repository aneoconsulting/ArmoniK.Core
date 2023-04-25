variable "container_name" {
  type = string
}

variable "image" {
  type = string
}

variable "tag" {
  type = string
}

variable "network" {
  type = string
}

variable "port" {
  type = number
}

variable "tls" {
  type = bool
}

variable "mtls" {
  type = bool
}

variable "submitter_url" {
  type = string
}

variable "submitter_port" {
  type = number
}

variable "log_driver" {
  type = object({
    name    = string,
    address = string,
  })
}

variable "submitter_image_id" {
  type        = string
  description = "For dependencies"
}
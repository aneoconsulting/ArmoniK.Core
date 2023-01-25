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

variable "minio_parameters" {
  description = "Parameters of minio"
  type = object({
  host               = string
  port               = number
  login              = string
  password           = string
  bucket_name        = string
  })
}
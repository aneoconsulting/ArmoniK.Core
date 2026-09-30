variable "image" {
  type = string
}

variable "network" {
  type = object({
    name   = string
    driver = string
  })
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

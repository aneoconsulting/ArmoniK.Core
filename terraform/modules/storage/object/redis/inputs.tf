variable "image" {
  type = string
}

variable "network" {
  type = object({
    name   = string
    driver = string
  })
}

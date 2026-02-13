variable "image" {
  type = string
}

variable "network" {
  type = object({
    name   = string
    driver = string
  })
}

variable "exposed_ports" {
  type = object({
    api       = number,
    ingestion = number,
  })
  default = {
    api       = 4080
    ingestion = 5341
  }
}

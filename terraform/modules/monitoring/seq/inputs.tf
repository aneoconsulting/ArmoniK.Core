variable "image" {
  type = string
}

variable "network" {
  type = string
}

variable "exposed_ports" {
  type = object({
    api       = number,
    ingestion = number,
  })
  default = {
    api       = 80
    ingestion = 5341
  }
}
variable "image" {
  type = string
}

variable "network" {
  type = string

}

variable "exposed_ports" {
  type = object({
    tempo     = number,
    oltp_grpc = number,
    oltp_http = number,
    #unknown = number,  #modify this name!!
  })
  default = {
    tempo     = 3200
    oltp_grpc = 4317
    oltp_http = 4318
    #unknown = 55681
  }
}
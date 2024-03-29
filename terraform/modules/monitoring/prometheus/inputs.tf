variable "image" {
  type = string
}

variable "network" {
  type = string

}

variable "exposed_port" {
  type    = number
  default = 9090

}

variable "polling_agent_names" {
  type    = set(string)
  default = []
}

variable "submitter_names" {
  type    = set(string)
  default = []
}

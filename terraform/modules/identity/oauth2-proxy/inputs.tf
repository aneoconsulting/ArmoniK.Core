variable "image" {
  type = string
}

variable "network" {
  type = object({
    name   = string
    driver = string
  })
}

variable "issuer" {
  description = "OIDC issuer whose tokens oauth2-proxy validates"
  type        = string
}

variable "client_id" {
  description = "Client the tokens are issued for, checked as their audience"
  type        = string
}

variable "groups_claim" {
  description = "Claim answered as the groups of the caller"
  type        = string
}

variable "log_driver" {
  type = object({
    name     = string,
    log_opts = map(string),
  })
}

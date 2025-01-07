variable "core_tag" {
  type = string
}

variable "container_name" {
  type = string
}

variable "docker_image" {
  type = string
}

variable "network" {
  type = string
}

variable "generated_env_vars" {
  type = map(string)
}

variable "mounts" {
  type = map(string)
}

variable "redis_base_path" {
  description = "Chemin de base pour les certificats Redis"
  type        = string
  default     = ""
}
variable "certs_path" {
  description = "Chemin local vers les certificats Redis"
  type        = string
  default     = ""
}
variable "default_certs_path" {
  description = "Chemin par défaut des certificats Redis"
  type        = string
  default     = "/../../storage/object/redis/generated/redis/certs"
}

variable "volumes" {
  type = map(string)
  default = {
    "redis-data" = "/data"
  }
}

variable "log_driver" {
  type = object({
    name     = string,
    log_opts = map(string),
  })
}
variable "redis_container_id" {
  type    = string
  default = null
}
variable "object_storage" {
  type = object({
    name  = string
    image = optional(string)
    host  = optional(string)
    port  = optional(number)
    login = optional(string)
    password = optional(string)
  })
}

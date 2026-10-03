variable "image" {
  type = string
}

variable "network" {
  type = object({
    name   = string
    driver = string
  })
}

variable "queue_envs" {
  type = object({
    user         = string,
    password     = string,
    host         = string,
    port         = number,
    max_priority = number,
    max_retries  = number,
    link_credit  = number,
  })
}

variable "queue_list" {
  type = list(string)
}

variable "exposed_ports" {
  type = object({
    connection = number,
  })
  default = {
    connection = 8080
  }
}

variable "windows" {
  type = bool
}

variable "affinity" {
  type        = bool
  default     = true
  description = "Task and data affinity; requires the agent cache (Pollster__CacheEvictionThreshold > 0, set by the compute plane)"
}

variable "log_level" {
  type    = string
  default = "info"
}

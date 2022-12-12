variable "use_local_image" {
  type    = bool
  default = false
}

variable "dockerfile_path" {
  type = string
}

variable "context_path" {
  type = string
}

variable "image_name" {
  type = string
}

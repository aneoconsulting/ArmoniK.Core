terraform {
  required_providers {
    docker = {
      source  = "registry.opentofu.org/kreuzwerker/docker"
      version = ">= 3.0.2"
    }
  }
}
terraform {
  required_providers {
    docker = {
      source  = "registry.opentofu.org/kreuzwerker/docker"
      version = ">= 4.6.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "0.14.1"
    }
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.4.0"
    }
  }
}

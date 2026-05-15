terraform {
  required_providers {
    docker = {
      source  = "registry.opentofu.org/kreuzwerker/docker"
      version = ">= 4.4.0"
    }
    pkcs12 = {
      source  = "chilicat/pkcs12"
      version = ">= 0.4.0"
    }
  }
}
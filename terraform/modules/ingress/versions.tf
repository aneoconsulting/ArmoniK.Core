terraform {
  required_providers {
    docker = {
      source  = "registry.opentofu.org/kreuzwerker/docker"
      version = ">= 4.2.0"
    }
    pkcs12 = {
      source  = "chilicat/pkcs12"
      version = ">= 0.3.2"
    }
  }
}
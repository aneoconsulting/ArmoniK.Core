terraform {
  required_providers {
    docker = {
      source  = "registry.opentofu.org/kreuzwerker/docker"
      version = ">= 3.0.2"
    }
    pkcs12 = {
      source  = "chilicat/pkcs12"
      version = ">= 0.2.5"
    }
  }
}
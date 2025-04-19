terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = ">= 3.3.0"
    }
    pkcs12 = {
      source  = "chilicat/pkcs12"
      version = ">= 0.2.5"
    }
  }
}
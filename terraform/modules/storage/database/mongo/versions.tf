terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = ">= 3.6.2"
    }
    time = {
      source  = "hashicorp/time"
      version = "0.13.1"
    }
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.2.0"
    }
  }
}

terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = ">= 3.0.2"
    }
    time = {
      source  = "hashicorp/time"
      version = "0.12.1"
    }
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.0.4"
    }
  }
}

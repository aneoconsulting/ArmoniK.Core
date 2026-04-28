terraform {
  required_providers {
    docker = {
      source  = "registry.opentofu.org/kreuzwerker/docker"
      version = "~> 4.2.0"
    }
    random = {
      source  = "registry.opentofu.org/hashicorp/random"
      version = "~> 3.8"
    }
    external = {
      source  = "registry.opentofu.org/hashicorp/external"
      version = "~> 2.3"
    }
    pkcs12 = {
      source  = "chilicat/pkcs12"
      version = ">= 0.3.2"
    }
  }
}

provider "docker" {

}

provider "pkcs12" {}

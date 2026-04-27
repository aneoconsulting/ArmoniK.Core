terraform {
  required_providers {
    docker = {
      source  = "registry.opentofu.org/kreuzwerker/docker"
      version = "~> 3.9.0"
    }
    random = {
      source  = "registry.opentofu.org/hashicorp/random"
      version = "~> 3.8"
    }
    external = {
      source  = "registry.opentofu.org/hashicorp/external"
      version = "~> 2.3"
    }
  }
}

provider "docker" {

}

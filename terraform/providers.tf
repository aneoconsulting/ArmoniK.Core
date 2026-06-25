terraform {
  required_providers {
    docker = {
      source  = "registry.opentofu.org/kreuzwerker/docker"
      version = "~> 4.5.0"
    }
    random = {
      source  = "registry.opentofu.org/hashicorp/random"
      version = "~> 3.9"
    }
    external = {
      source  = "registry.opentofu.org/hashicorp/external"
      version = "~> 2.4"
    }
  }
}

provider "docker" {

}

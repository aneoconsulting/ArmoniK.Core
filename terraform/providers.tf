terraform {
  required_providers {
    docker = {
      source  = "registry.opentofu.org/kreuzwerker/docker"
      version = "~> 3.5.0"
    }
  }
}

provider "docker" {

}

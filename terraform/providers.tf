terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.3.0"
    }
  }
}

provider "docker" {

}

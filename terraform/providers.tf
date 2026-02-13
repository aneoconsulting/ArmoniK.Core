terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "3.1.1"
    }
  }
}

provider "docker" {

}

resource "docker_network" "armonik-net" {
  name = "armonik-net"
}

resource "docker_network" "armonik-backend" {
  name = "armonik-backend"
}

resource "docker_network" "armonik-monitoring" {
  name = "armonik-monitoring"
}
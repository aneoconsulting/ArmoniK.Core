resource "docker_network" "armonik_net" {
  name = "armonik_net"
}

resource "docker_network" "armonik_backend" {
  name = "armonik_backend"
}

resource "docker_network" "armonik_monitoring" {
  name = "armonik_monitoring"
}
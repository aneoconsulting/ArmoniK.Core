resource "docker_network" "armonik" {
  name   = "armonik_network"
  driver = "nat"
}

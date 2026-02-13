resource "docker_network" "armonik" {
  count = var.windows ? 0 : 1
  name  = "armonik_network"
}

data "docker_network" "armonik" {
  count = var.windows ? 1 : 0
  name  = "nat"
}

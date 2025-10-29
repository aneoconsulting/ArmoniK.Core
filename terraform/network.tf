resource "docker_network" "armonik" {
  name   = "armonik_network"
  driver = var.windows ? "transparent" : null
}

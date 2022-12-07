locals {
  vol-replicas = toset([for s in range(var.num-replicas) : tostring(s)])
}

resource "docker_volume" "socket-vol" {
  for_each = local.vol-replicas
  name     = "socket-vol${each.value}"
}

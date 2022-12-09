locals {
  vol_replicas = toset([for s in range(var.num_replicas) : tostring(s)])
}

resource "docker_volume" "socket_vol" {
  for_each = local.vol_replicas
  name     = "socket_vol${each.value}"
}

locals {
  replicas = toset([for s in range(var.num_replicas) : tostring(s)])
}


module "submitter" {
  source         = "./modules/submitter"
  container_name = "armonik.control.submitter"
  core_tag       = var.core_tag
  docker_image   = var.armonik_submitter_image
  network        = docker_network.armonik_net.name

  implicit_dependencies = [
  ]

  # handle fluentbit dependency as explicit, deployment fails otherwise
  depends_on = [
    docker_container.database,
    docker_container.queue,
    docker_container.object,
    docker_container.zipkin,
    docker_container.fluentbit
  ]
}

module "pollingagent" {
  source          = "./modules/pollingAgent"
  for_each        = local.replicas
  container_name  = "armonik.compute.pollingagent${each.value}"
  replica_counter = each.key
  core_tag        = var.core_tag
  docker_image    = var.armonik_pollingagent_image
  network         = docker_network.armonik_net.name
  socket_vol      = docker_volume.socket_vol[each.key].name

  implicit_dependencies = [
  ]

  # handle fluentbit dependency as explicit, deployment fails otherwise
  depends_on = [
    docker_container.database,
    docker_container.queue,
    docker_container.object,
    docker_container.zipkin,
    docker_container.fluentbit
  ]
}

module "worker" {
  source          = "./modules/worker"
  for_each        = local.replicas
  container_name  = "armonik.compute.worker${each.value}"
  replica_counter = each.key
  core_tag        = var.core_tag
  docker_image    = var.armonik_worker_image
  network         = docker_network.armonik_net.name
  socket_vol      = docker_volume.socket_vol[each.key].name

  implicit_dependencies = [
  ]

  # handle fluentbit dependency as explicit, deployment fails otherwise
  depends_on = [
    docker_container.database,
    docker_container.queue,
    docker_container.object,
    docker_container.zipkin,
    docker_container.fluentbit
  ]
}

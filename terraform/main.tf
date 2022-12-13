locals {
  replicas = toset([for s in range(var.num_replicas) : tostring(s)])
}

module "fluenbit" {
  source  = "./modules/monitoring/fluentbit"
  image   = var.log_driver
  network = docker_network.armonik.name
}

module "seq" {
  source  = "./modules/monitoring/seq"
  image   = var.seq_image
  network = docker_network.armonik.name
}

module "zipkin" {
  source  = "./modules/monitoring/zipkin"
  image   = var.zipkin_image
  network = docker_network.armonik.name
}

module "database" {
  source  = "./modules/storage/mongo"
  image   = var.database_image
  network = docker_network.armonik.name
}

module "object" {
  source  = "./modules/storage/redis"
  image   = var.object_image
  network = docker_network.armonik.name
}

module "queue" {
  source  = "./modules/queue/activemq"
  image   = var.queue_image
  network = docker_network.armonik.name
}

module "submitter" {
  source         = "./modules/submitter"
  container_name = "armonik.control.submitter"
  core_tag       = var.core_tag
  docker_image   = var.armonik_submitter_image
  network        = docker_network.armonik.name
  zipkin_uri     = module.zipkin.zipkin_uri
  log_driver     = module.fluenbit.log_driver
}

module "compute_plane" {
  source                       = "./modules/compute_plane"
  for_each                     = local.replicas
  polling_agent_container_name = "armonik.compute.pollingagent${each.value}"
  worker_container_name        = "armonik.compute.worker${each.value}"
  replica_counter              = each.key
  core_tag                     = var.core_tag
  polling_agent_image          = var.armonik_pollingagent_image
  worker_image                 = var.armonik_worker_image
  network                      = docker_network.armonik.name
  zipkin_uri                   = module.zipkin.zipkin_uri
  log_driver                   = module.fluenbit.log_driver
}

module "metrics_exporter" {
  source          = "./modules/monitoring/metrics"
  tag             = var.core_tag
  image           = var.armonik_metrics_image
  use_local_image = var.use_local_image
  network         = docker_network.armonik.name
  log_driver      = module.fluenbit.log_driver
}

module "partition_metrics_exporter" {
  source          = "./modules/monitoring/partition_metrics"
  tag             = var.core_tag
  image           = var.armonik_partition_metrics_image
  use_local_image = var.use_local_image
  network         = docker_network.armonik.name
  log_driver      = module.fluenbit.log_driver
}
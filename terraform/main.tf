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
  source         = "./modules/storage/mongo"
  image          = var.database_image
  network        = docker_network.armonik.name
  mongodb_params = var.mongodb_params
}

module "object" {
  source  = "./modules/storage/redis"
  image   = var.object_image
  network = docker_network.armonik.name
}

module "queue" {
  source        = "./modules/queue/activemq"
  queue_storage = var.queue_storage
  image         = var.queue_image
  network       = docker_network.armonik.name
}

module "submitter" {
  source            = "./modules/submitter"
  container_name    = local.submitter.name
  core_tag          = local.submitter.tag
  docker_image      = local.submitter.image
  log_level         = local.submitter.log_level
  dev_env           = local.submitter.aspnet_core_env
  object_storage    = local.submitter.object_storage
  network           = docker_network.armonik.name
  database_env_vars = module.database.database_env_vars
  queue_env_vars    = module.queue.queue_env_vars
  object_env_vars   = module.object.object_env_vars
  zipkin_uri        = module.zipkin.zipkin_uri
  object_driver     = module.object.object_driver
  log_driver        = module.fluenbit.log_driver
}

module "compute_plane" {
  source            = "./modules/compute_plane"
  for_each          = local.replicas
  replica_counter   = each.key
  core_tag          = local.compute_plane.tag
  dev_env           = local.compute_plane.aspnet_core_env
  log_level         = local.compute_plane.log_level
  polling_agent     = local.compute_plane.polling_agent
  worker            = local.compute_plane.worker
  queue_env_vars    = module.queue.queue_env_vars
  object_env_vars   = module.object.object_env_vars
  database_env_vars = module.database.database_env_vars
  network           = docker_network.armonik.name
  zipkin_uri        = module.zipkin.zipkin_uri
  object_driver     = module.object.object_driver
  log_driver        = module.fluenbit.log_driver
}

module "metrics_exporter" {
  source          = "./modules/monitoring/metrics"
  tag             = var.core_tag
  image           = var.armonik_metrics_image
  use_local_image = var.use_local_image
  network         = docker_network.armonik.name
  mongodb_params  = var.mongodb_params
  db_driver       = module.database.database_driver # TODO: Replace by database_env_vars
  log_driver      = module.fluenbit.log_driver
}

module "partition_metrics_exporter" {
  source          = "./modules/monitoring/partition_metrics"
  tag             = var.core_tag
  image           = var.armonik_partition_metrics_image
  use_local_image = var.use_local_image
  network         = docker_network.armonik.name
  mongodb_params  = var.mongodb_params
  db_driver       = module.database.database_driver # TODO: Replace by database_env_vars
  log_driver      = module.fluenbit.log_driver
}
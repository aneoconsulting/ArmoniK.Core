module "fluenbit" {
  source  = "./modules/monitoring/fluentbit"
  image   = var.log_driver_image
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

module "queue_rabbitmq" {
  source        = "./modules/queue/rabbitmq"
  count         = var.queue_storage.broker.name == "rabbitmq" ? 1 : 0
  queue_storage = var.queue_storage
  image         = var.queue_storage.broker.image
  network       = docker_network.armonik.name
}

module "queue_activemq" {
  source        = "./modules/queue/activemq"
  count         = var.queue_storage.broker.name == "activemq" ? 1 : 0
  queue_storage = var.queue_storage
  image         = var.queue_storage.broker.image
  network       = docker_network.armonik.name
}


module "submitter" {
  source            = "./modules/submitter"
  container_name    = local.submitter.name
  core_tag          = local.submitter.tag
  docker_image      = local.submitter.image
  log_level         = local.submitter.log_level
  dev_env           = local.submitter.aspnet_core_env
  network           = docker_network.armonik.name
  database_env_vars = module.database.database_env_vars
  queue_env_vars    = local.queue_env_vars
  object_env_vars   = module.object.object_env_vars
  zipkin_uri        = module.zipkin.zipkin_uri
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
  queue_env_vars    = local.queue_env_vars
  object_env_vars   = module.object.object_env_vars
  database_env_vars = module.database.database_env_vars
  network           = docker_network.armonik.name
  zipkin_uri        = module.zipkin.zipkin_uri
  log_driver        = module.fluenbit.log_driver
}

module "metrics_exporter" {
  source            = "./modules/monitoring/metrics"
  tag               = var.core_tag
  image             = var.armonik_metrics_image
  use_local_image   = var.use_local_image
  network           = docker_network.armonik.name
  dev_env           = local.compute_plane.aspnet_core_env
  log_level         = local.compute_plane.log_level
  database_env_vars = module.database.database_env_vars
  log_driver        = module.fluenbit.log_driver
}

module "partition_metrics_exporter" {
  source            = "./modules/monitoring/partition_metrics"
  tag               = var.core_tag
  image             = var.armonik_partition_metrics_image
  use_local_image   = var.use_local_image
  network           = docker_network.armonik.name
  dev_env           = local.compute_plane.aspnet_core_env
  log_level         = local.compute_plane.log_level
  database_env_vars = module.database.database_env_vars
  metrics_env_vars  = module.metrics_exporter.metrics_env_vars
  log_driver        = module.fluenbit.log_driver
}
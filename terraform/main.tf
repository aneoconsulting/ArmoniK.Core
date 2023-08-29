module "fluenbit" {
  source  = "./modules/monitoring/fluentbit"
  image   = var.log_driver_image
  network = docker_network.armonik.id
}

module "seq" {
  source  = "./modules/monitoring/seq"
  image   = var.seq_image
  network = docker_network.armonik.id
}

module "grafana" {
  source  = "./modules/monitoring/grafana"
  image   = var.grafana_image
  network = docker_network.armonik.id
}

module "prometheus" {
  source  = "./modules/monitoring/prometheus"
  image   = var.prometheus_image
  network = docker_network.armonik.id
}

module "database" {
  source         = "./modules/storage/database/mongo"
  image          = var.database_image
  network        = docker_network.armonik.id
  mongodb_params = var.mongodb_params
  partition_list = local.partition_list
}

module "object_redis" {
  source  = "./modules/storage/object/redis"
  count   = var.object_storage.name == "redis" ? 1 : 0
  image   = var.object_storage.image
  network = docker_network.armonik.id
}

module "object_minio" {
  source      = "./modules/storage/object/minio"
  count       = var.object_storage.name == "minio" ? 1 : 0
  image       = var.object_storage.image
  host        = var.object_storage.host
  port        = var.object_storage.port
  login       = var.object_storage.login
  password    = var.object_storage.password
  bucket_name = var.object_storage.bucket_name
  network     = docker_network.armonik.id
}

module "object_local" {
  source = "./modules/storage/object/local"
  count  = var.object_storage.name == "local" ? 1 : 0
}

module "queue_rabbitmq" {
  source     = "./modules/storage/queue/rabbitmq"
  count      = var.queue_storage.name == "rabbitmq" ? 1 : 0
  queue_envs = var.queue_env_vars
  image      = var.queue_storage.image
  protocol   = var.queue_storage.protocol
  network    = docker_network.armonik.id
}

module "queue_activemq" {
  source     = "./modules/storage/queue/activemq"
  count      = var.queue_storage.name == "activemq" ? 1 : 0
  queue_envs = var.queue_env_vars
  image      = var.queue_storage.image
  network    = docker_network.armonik.id
}

module "queue_artemis" {
  source     = "./modules/storage/queue/artemis"
  count      = var.queue_storage.name == "artemis" ? 1 : 0
  queue_envs = var.queue_env_vars
  image      = var.queue_storage.image
  network    = docker_network.armonik.id
}

module "queue_none" {
  source = "./modules/storage/queue/none"
  count  = var.queue_storage.name == "none" ? 1 : 0
}

module "submitter" {
  source             = "./modules/submitter"
  container_name     = local.submitter.name
  core_tag           = local.submitter.tag
  use_local_image    = var.use_local_image
  docker_image       = local.submitter.image
  network            = docker_network.armonik.id
  generated_env_vars = local.environment
  log_driver         = module.fluenbit.log_driver
  volumes            = local.volumes
}

module "compute_plane" {
  source             = "./modules/compute_plane"
  for_each           = local.replicas
  replica_counter    = each.key
  num_partitions     = var.num_partitions
  core_tag           = local.compute_plane.tag
  use_local_image    = var.use_local_image
  polling_agent      = local.compute_plane.polling_agent
  worker             = local.compute_plane.worker
  generated_env_vars = local.environment
  volumes            = local.volumes
  network            = docker_network.armonik.id
  log_driver         = module.fluenbit.log_driver
}

module "metrics_exporter" {
  source             = "./modules/monitoring/metrics"
  tag                = var.core_tag
  image              = var.armonik_metrics_image
  use_local_image    = var.use_local_image
  network            = docker_network.armonik.id
  generated_env_vars = local.environment
  log_driver         = module.fluenbit.log_driver
}

module "partition_metrics_exporter" {
  source             = "./modules/monitoring/partition_metrics"
  tag                = var.core_tag
  image              = var.armonik_partition_metrics_image
  use_local_image    = var.use_local_image
  network            = docker_network.armonik.id
  generated_env_vars = local.environment
  metrics_env_vars   = module.metrics_exporter.metrics_env_vars
  log_driver         = module.fluenbit.log_driver
}

module "ingress" {
  source   = "./modules/ingress"
  for_each = var.ingress.configs
  container = {
    name  = each.key,
    image = var.ingress.image
    tag   = var.ingress.tag
  }
  tls        = each.value.tls
  mtls       = each.value.mtls
  port       = each.value.port
  network    = docker_network.armonik.id
  submitter  = module.submitter
  log_driver = module.fluenbit.log_driver
}

module "tracing" {
  source               = "./modules/tracing"
  network              = docker_network.armonik.id
  count                = var.tracing_exporters == null ? 0 : 1
  exporters            = var.tracing_exporters
  zipkin_image         = var.zipkin_image
  otel_collector_image = var.otel_collector_image
  ingestion_ports      = var.tracing_ingestion_ports
}
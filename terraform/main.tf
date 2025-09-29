module "fluenbit" {
  source  = "./modules/monitoring/fluentbit"
  image   = var.log_driver_image
  network = docker_network.armonik.id
}

module "seq" {
  count   = var.enable_seq ? 1 : 0
  source  = "./modules/monitoring/seq"
  image   = var.seq_image
  network = docker_network.armonik.id
}

module "grafana" {
  count   = var.enable_grafana ? 1 : 0
  source  = "./modules/monitoring/grafana"
  image   = var.grafana_image
  network = docker_network.armonik.id
}

module "prometheus" {
  count               = var.enable_prometheus ? 1 : 0
  source              = "./modules/monitoring/prometheus"
  image               = var.prometheus_image
  network             = docker_network.armonik.id
  polling_agent_names = local.polling_agent_names
  submitter_names     = [var.submitter.name]
}

module "database" {
  source         = "./modules/storage/database/mongo"
  image          = var.database_image
  network        = docker_network.armonik.id
  mongodb_params = var.mongodb_params
  windows        = var.windows
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
  source     = "./modules/storage/object/local"
  count      = var.object_storage.name == "local" ? 1 : 0
  local_path = var.object_storage.local_storage_path
}

module "object_embed" {
  source = "./modules/storage/object/embed"
  count  = var.object_storage.name == "embed" ? 1 : 0
}

module "queue_rabbitmq" {
  source     = "./modules/storage/queue/rabbitmq"
  count      = var.queue_storage.name == "rabbitmq" ? 1 : 0
  queue_envs = var.queue_env_vars
  image      = var.queue_storage.image
  protocol   = var.queue_storage.protocol
  network    = docker_network.armonik.id
  windows    = var.windows
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

module "queue_pubsub" {
  source     = "./modules/storage/queue/pubsub"
  count      = var.queue_storage.name == "pubsub" ? 1 : 0
  queue_envs = var.queue_env_vars
  image      = var.queue_storage.image
  network    = docker_network.armonik.id
}

module "queue_nats" {
  source     = "./modules/storage/queue/nats"
  count      = var.queue_storage.name == "nats" ? 1 : 0
  queue_envs = var.queue_env_vars
  image      = var.queue_storage.image
  network    = docker_network.armonik.id
}

module "queue_sqs" {
  source     = "./modules/storage/queue/sqs"
  count      = var.queue_storage.name == "sqs" ? 1 : 0
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
  docker_image       = local.submitter.image
  network            = docker_network.armonik.id
  generated_env_vars = local.environment
  log_driver         = module.fluenbit.log_driver
  volumes            = local.volumes
  mounts             = local.mounts
  container_init     = var.container_init
}

module "compute_plane" {
  source             = "./modules/compute_plane"
  for_each           = local.replicas
  replica_counter    = each.key
  num_partitions     = var.num_partitions
  core_tag           = local.compute_plane.tag
  polling_agent      = local.compute_plane.polling_agent
  worker             = local.compute_plane.worker
  socket_type        = var.socket_type
  generated_env_vars = local.environment
  volumes            = local.volumes
  network            = docker_network.armonik.id
  log_driver         = module.fluenbit.log_driver
  mounts             = local.mounts
  container_init     = var.container_init
  windows            = var.windows
}

module "metrics_exporter" {
  source             = "./modules/monitoring/metrics"
  tag                = var.core_tag
  image              = var.armonik_metrics_image
  network            = docker_network.armonik.id
  generated_env_vars = local.environment
  log_driver         = module.fluenbit.log_driver
  mounts             = local.mounts
  container_init     = var.container_init
}

module "partition_metrics_exporter" {
  source             = "./modules/monitoring/partition_metrics"
  tag                = var.core_tag
  image              = var.armonik_partition_metrics_image
  network            = docker_network.armonik.id
  generated_env_vars = local.environment
  metrics_env_vars   = module.metrics_exporter.metrics_env_vars
  log_driver         = module.fluenbit.log_driver
  mounts             = local.mounts
  container_init     = var.container_init
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

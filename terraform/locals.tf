locals {
  partitions = toset([for s in range(var.num_partitions) : tostring(s)])
  replicas   = toset([for s in range(var.num_replicas) : tostring(s)])
  logging_env_vars = { "Serilog__MinimumLevel" = "${var.serilog_level}",
    "ASPNETCORE_ENVIRONMENT" = "${var.aspnet_core_env}"
  }
  worker            = merge(var.compute_plane.worker, { image = var.worker_image, docker_file_path = var.worker_docker_file_path })
  queue             = one(concat(module.queue_activemq, module.queue_rabbitmq))
  queue_env_vars    = local.queue.generated_env_vars
  object            = one(concat(module.object_redis, module.object_minio, module.object_local))
  object_env_vars   = local.object.generated_env_vars
  database_env_vars = module.database.generated_env_vars
  environment       = merge(local.queue_env_vars, local.object_env_vars, local.database_env_vars, local.logging_env_vars)
  submitter         = merge(var.submitter, { tag = var.core_tag })
  compute_plane     = merge(var.compute_plane, { tag = var.core_tag }, { worker = local.worker })
  partition_list    = { for i in local.partitions : i => merge(var.partition_data, { _id = "${var.partition_data._id}${i}" }) }
}

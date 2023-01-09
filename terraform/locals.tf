locals {
  replicas = toset([for s in range(var.num_replicas) : tostring(s)])
  logging_env_vars = { "Serilog__MinimumLevel" = "${var.logging_env_vars.log_level}",
    "ASPNETCORE_ENVIRONMENT" = "${var.logging_env_vars.aspnet_core_env}"
  }
  queue             = one(concat(module.queue_activemq, module.queue_rabbitmq))
  queue_env_vars    = local.queue.queue_env_vars
  object            = one(concat(module.object_redis, module.object_local))
  object_env_vars   = local.object.object_env_vars
  database_env_vars = module.database.database_env_vars
  environment       = merge(local.queue_env_vars, local.object_env_vars, local.database_env_vars, local.logging_env_vars)
  submitter         = merge(var.submitter, { tag = var.core_tag })
  compute_plane     = merge(var.compute_plane, { tag = var.core_tag })
}

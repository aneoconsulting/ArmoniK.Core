locals {
  partitions = toset([for s in range(var.num_partitions) : tostring(s)])
  replicas   = toset([for s in range(var.num_replicas) : tostring(s)])
  logging_env_vars = { "Serilog__MinimumLevel" = "${var.serilog.loggin_level}",
    "Serilog__MinimumLevel__Override__Microsoft.AspNetCore.Hosting.Diagnostics"        = "${var.serilog.loggin_level_routing}",
    "Serilog__MinimumLevel__Override__Microsoft.AspNetCore.Routing.EndpointMiddleware" = "${var.serilog.loggin_level_routing}",
    "Serilog__MinimumLevel__Override__Serilog.AspNetCore.RequestLoggingMiddleware"     = "${var.serilog.loggin_level_routing}",
    "ASPNETCORE_ENVIRONMENT"                                                           = "${var.aspnet_core_env}"
  }
  worker         = merge(var.compute_plane.worker, { image = var.worker_image, docker_file_path = var.worker_docker_file_path })
  queue          = one(concat(module.queue_activemq, module.queue_rabbitmq, module.queue_artemis))
  database       = module.database
  object         = one(concat(module.object_redis, module.object_minio, module.object_local))
  environment    = merge(local.queue.generated_env_vars, local.object.generated_env_vars, local.database.generated_env_vars, local.logging_env_vars)
  volumes        = local.object.volumes
  submitter      = merge(var.submitter, { tag = var.core_tag })
  compute_plane  = merge(var.compute_plane, { tag = var.core_tag }, { worker = local.worker })
  partition_list = { for i in local.partitions : i => merge(var.partition_data, { _id = "${var.partition_data._id}${i}" }) }
}

locals {
  partitions = toset([for s in range(var.num_partitions) : tostring(s)])
  replicas   = toset([for s in range(var.num_replicas) : tostring(s)])
  queue_list = [for i in sort(tolist(local.partitions)) : "${var.partition_data.PartitionId}${i}"]
  logging_env_vars = { "Serilog__MinimumLevel" = "${var.serilog.loggin_level}",
    "Serilog__MinimumLevel__Override__Microsoft.AspNetCore.Hosting.Diagnostics"           = "${var.serilog.loggin_level_routing}",
    "Serilog__MinimumLevel__Override__Microsoft.AspNetCore.Routing"                       = "${var.serilog.loggin_level_routing}",
    "Serilog__MinimumLevel__Override__Serilog.AspNetCore.RequestLoggingMiddleware"        = "${var.serilog.loggin_level_routing}",
    "Serilog__MinimumLevel__Override__Microsoft.AspNetCore.Server.Kestrel"                = "${var.serilog.loggin_level_routing}",
    "Serilog__MinimumLevel__Override__Grpc.AspNetCore.Server.ServerCallHandler"           = "${var.serilog.loggin_level_routing}",
    "Serilog__MinimumLevel__Override__Microsoft.Extensions.Diagnostics.HealthChecks"      = "${var.serilog.loggin_level_routing}",
    "Serilog__MinimumLevel__Override__Microsoft.AspNetCore.Authorization"                 = "${var.serilog.loggin_level_routing}",
    "Serilog__MinimumLevel__Override__Microsoft.Extensions.Http.DefaultHttpClientFactory" = "${var.serilog.loggin_level_routing}",
    "ASPNETCORE_ENVIRONMENT"                                                              = "${var.aspnet_core_env}"
  }
  worker             = merge(var.compute_plane.worker, { image = var.worker_image })
  queue              = one(concat(module.queue_activemq, module.queue_rabbitmq, module.queue_artemis, module.queue_pubsub, module.queue_sqs, module.queue_nats, module.queue_none))
  database           = one(concat(module.database_mongo, module.database_postgresql))
  object             = one(concat(module.object_redis, module.object_seaweedfs, module.object_gcs, module.object_local, module.object_embed, module.object_null))
  partition_env_vars = { for i in local.partitions : "InitServices__Partitioning__Partitions__${i}" => jsonencode(merge(var.partition_data, { PartitionId = "${var.partition_data.PartitionId}${i}" })) }
  # Keycloak issues tokens: they are read whenever requests are authorized, never when authorization is disabled
  authentication_methods = [for method in split(",", replace(var.authorization.authentication, " ", "")) : method if method != "None"]
  authentication         = join(",", distinct(concat(local.authentication_methods, length(local.authentication_methods) != 0 && var.enable_keycloak ? ["Token"] : [])))
  authorization_env_vars = local.authentication != "" ? merge(
    {
      "Authorization__Authentication" = local.authentication
      "Authorization__ConfigRevision" = tostring(var.authorization.config_revision)
    },
    merge(concat([{}], [for i, grant in var.authorization.grants : merge({
      "Authorization__Grants__${i}__Id"            = grant.id
      "Authorization__Grants__${i}__Principal"     = grant.principal
      "Authorization__Grants__${i}__PermissionSet" = grant.permission_set
    }, grant.condition == null ? {} : { "Authorization__Grants__${i}__Condition" = grant.condition })])...),
    merge(concat([{}], [for i, set in var.authorization.permission_sets : merge(
      { "Authorization__PermissionSets__${i}__Name" = set.name },
      { for j, permission in set.permissions : "Authorization__PermissionSets__${i}__Permissions__${j}" => permission }
    )])...)
  ) : {}
  # Keycloak issues a stable `sub`, and is reached over plain HTTP in the network
  keycloak_env_vars = var.enable_keycloak ? {
    "Identity__Authority"            = one(module.keycloak[*].authority)
    "Identity__Audience"             = one(module.keycloak[*].audience)
    "Identity__SubjectClaim"         = "sub"
    "Identity__RequireHttpsMetadata" = "false"
  } : {}
  env_maps = concat([
    local.queue.generated_env_vars,
    local.object.generated_env_vars,
    local.database.generated_env_vars,
    local.logging_env_vars,
    var.custom_env_vars,
    local.partition_env_vars,
    local.authorization_env_vars,
    local.keycloak_env_vars
  ], module.tracing[*].generated_env_vars)
  mounts              = merge(local.database.core_mounts, local.object.core_mounts, local.queue.core_mounts)
  environment         = merge(local.env_maps...)
  volumes             = local.object.volumes
  submitter           = merge(var.submitter, { tag = var.core_tag })
  compute_plane       = merge(var.compute_plane, { tag = var.core_tag }, { worker = local.worker })
  polling_agent_names = toset([for v in module.compute_plane : v.polling_agent_name])
  network             = one(concat(docker_network.armonik, data.docker_network.armonik))
}
resource "local_file" "queue_env" {
  filename = "${path.root}/generated/queue_env.sh"
  content  = join("\n", [for k, v in local.queue.generated_env_vars : "export ${k}=${v}"])
}

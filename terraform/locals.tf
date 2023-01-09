locals {
  replicas        = toset([for s in range(var.num_replicas) : tostring(s)])
  queue           = one(concat(module.queue_activemq, module.queue_rabbitmq))
  queue_env_vars  = local.queue.queue_env_vars
  object          = one(concat(module.object_redis, module.object_local))
  object_env_vars = local.object.object_env_vars
  submitter       = merge(var.submitter, { tag = var.core_tag })
  compute_plane   = merge(var.compute_plane, { tag = var.core_tag })
}

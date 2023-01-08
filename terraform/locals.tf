locals {
  replicas       = toset([for s in range(var.num_replicas) : tostring(s)])
  queue_env_vars = var.queue_storage.broker.name == "activemq" ? module.queue_activemq[0].queue_env_vars : module.queue_rabbitmq[0].queue_env_vars
  submitter      = merge(var.submitter,  { tag = var.core_tag })
  compute_plane  = merge(var.compute_plane, { tag = var.core_tag })
}
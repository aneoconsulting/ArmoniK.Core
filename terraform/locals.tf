locals {
  replicas      = toset([for s in range(var.num_replicas) : tostring(s)])
  submitter     = merge(var.submitter, { queue_storage = var.queue_storage }, { tag = var.core_tag })
  compute_plane = merge(var.compute_plane, { queue_storage = var.queue_storage }, { tag = var.core_tag })
}

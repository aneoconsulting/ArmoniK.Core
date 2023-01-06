locals {
  submitter     = merge(var.submitter, { queue_storage = var.queue_storage }, { tag = var.core_tag })
  compute_plane = merge(var.compute_plane, { queue_storage = var.queue_storage }, { tag = var.core_tag })
}

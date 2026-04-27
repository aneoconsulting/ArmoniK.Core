locals {
  # Mirrors the C# constant MaxInternalQueuePriority = 10 in PushQueueStorage
  max_internal_queue_priority = 10

  # Number of physical RabbitMQ queues per partition (one per priority band)
  nb_links = floor((var.queue_envs.max_priority - 1) / local.max_internal_queue_priority) + 1

  # Exchange name is the common prefix of partition names (e.g. "TestPartition0" → "TestPartition")
  exchange_name = replace(var.queue_list[0], "/\\d+$/", "")

  # Flat list of every (partition, priority-band) pair
  queue_items = flatten([
    for idx, partition in var.queue_list : [
      for j in range(local.nb_links) : {
        partition = partition
        idx       = idx
        level     = j
      }
    ]
  ])

  definitions = {
    rabbit_version = "4.0.0"
    users = [{
      name              = "guest"
      password_hash     = data.external.rabbit_guest_password_hash.result.hash
      hashing_algorithm = "rabbit_password_hashing_sha256"
      tags              = ["administrator"]
    }]
    vhosts = [{ name = "/" }]
    permissions = [{
      user      = "guest"
      vhost     = "/"
      configure = ".*"
      write     = ".*"
      read      = ".*"
    }]
    queues = [for item in local.queue_items : {
      name        = "${item.partition}q${item.level}"
      vhost       = "/"
      durable     = true
      auto_delete = false
      arguments   = {}
    }]
    exchanges = [{
      name        = local.exchange_name
      vhost       = "/"
      type        = "direct"
      durable     = true
      auto_delete = false
      arguments   = {}
    }]
    bindings = [for item in local.queue_items : {
      source           = local.exchange_name
      vhost            = "/"
      destination      = "${item.partition}q${item.level}"
      destination_type = "queue"
      routing_key      = tostring(item.idx * local.nb_links + item.level)
      arguments        = {}
    }]
    policies   = []
    parameters = []
  }

  definitions_json = jsonencode(local.definitions)
}

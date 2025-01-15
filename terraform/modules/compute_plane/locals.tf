locals {
  partition_chooser = var.replica_counter % var.num_partitions
  env = [
    "Pollster__MaxErrorAllowed=${var.polling_agent.max_error_allowed}",
    "Pollster__InternalCacheFolder=${var.polling_agent.shared_data}/internal",
    "Pollster__SharedCacheFolder=${var.polling_agent.shared_data}/shared",
    "InitWorker__WorkerCheckRetries=${var.polling_agent.worker_check_retries}",
    "InitWorker__WorkerCheckDelay=${var.polling_agent.worker_check_delay}",
    "Amqp__PartitionId=TestPartition${local.partition_chooser}",
    "PubSub__PartitionId=TestPartition${local.partition_chooser}",
  ]
  worker_tcp    = format("%s://%s:%s", "http", "${var.worker.name}${var.replica_counter}", "10667")
  worker_socket = format("%s/%s", var.polling_agent.shared_socket, "armonik_worker.sock")
  agent_tcp     = format("%s://%s:%s", "http", "${var.polling_agent.name}${var.replica_counter}", "10666")
  agent_socket  = format("%s/%s", var.polling_agent.shared_socket, "armonik_agent.sock")
  common_env = [
    "ComputePlane__WorkerChannel__SocketType=${var.socket_type}",
    "ComputePlane__WorkerChannel__Address=${var.socket_type == "tcp" ? local.worker_tcp : local.worker_socket}",
    "ComputePlane__AgentChannel__SocketType=${var.socket_type}",
    "ComputePlane__AgentChannel__Address=${var.socket_type == "tcp" ? local.agent_tcp : local.agent_socket}"
  ]
  gen_env            = [for k, v in var.generated_env_vars : "${k}=${v}"]
  polling_agent_name = "${var.polling_agent.name}${var.replica_counter}"
}

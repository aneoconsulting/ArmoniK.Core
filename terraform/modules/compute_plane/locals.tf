locals {
  test-cmd          = <<-EOF
    exec 3<>"/dev/tcp/localhost/1080" &&
    echo -en "GET /liveness HTTP/1.1\r\nHost: localhost:1080\r\nConnection: close\r\n\r\n">&3 &&
    grep Healthy <&3 &>/dev/null || exit 1
  EOF
  partition_chooser = var.replica_counter % var.num_partitions
  env = [
    "Pollster__MaxErrorAllowed=${var.polling_agent.max_error_allowed}",
    "InitWorker__WorkerCheckRetries=${var.polling_agent.worker_check_retries}",
    "InitWorker__WorkerCheckDelay=${var.polling_agent.worker_check_delay}",
    "Amqp__PartitionId=TestPartition${local.partition_chooser}",
    "PubSub__PartitionId=TestPartition${local.partition_chooser}",
  ]
  common_env = [
    "ComputePlane__WorkerChannel__SocketType=unixdomainsocket",
    "ComputePlane__WorkerChannel__Address=/cache/armonik_worker.sock",
    "ComputePlane__AgentChannel__SocketType=unixdomainsocket",
    "ComputePlane__AgentChannel__Address=/cache/armonik_agent.sock",
  ]
  gen_env = [for k, v in var.generated_env_vars : "${k}=${v}"]
}

locals {
  test-cmd = <<-EOF
    exec 3<>"/dev/tcp/localhost/1080"
    echo -en "GET /liveness HTTP/1.1\r\nHost: localhost:1080\r\nConnection: close\r\n\r\n">&3 &
    grep Healthy <&3 &>/dev/null || exit 1
    EOF
  env = [
    "Pollster__MaxErrorAllowed=${var.polling_agent.max_error_allowed}",
    "InitWorker__WorkerCheckRetries=${var.polling_agent.worker_check_retries}",
    "InitWorker__WorkerCheckDelay=${var.polling_agent.worker_check_retries}",
    "Zipkin__Uri=${var.zipkin_uri}",
    "Amqp__PartitionId=TestPartition${var.replica_counter}" #TODO: To be generated in module
  ]
  gen_env = [for k, v in var.generated_env_vars : "${k}=${v}"]
}
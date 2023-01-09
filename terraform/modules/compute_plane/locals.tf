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
    "Serilog__MinimumLevel=${var.log_level}",
    "Zipkin__Uri=${var.zipkin_uri}",
    "ASPNETCORE_ENVIRONMENT=${var.dev_env}",
    "Amqp__PartitionId=TestPartition${var.replica_counter}" #TODO: To be generated in module
  ]
  db_env     = [for k, v in var.database_env_vars : "${k}=${v}"]
  queue_env  = [for k, v in var.queue_env_vars : "${k}=${v}"]
  object_env = [for k, v in var.object_env_vars : "${k}=${v}"]
  gen_env    = concat(local.object_env, concat(local.db_env, local.queue_env))
}
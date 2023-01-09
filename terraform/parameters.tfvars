core_tag = "0.8.0"

use_local_image = false

num_replicas = 3

mongodb_params = {
  max_connection_pool_size = "500"
  min_polling_delay        = "00:00:01"
  max_polling_delay        = "00:00:10"
}

queue_storage = {
  protocol = "amqp1_0"
  broker = {
    name  = "activemq"
    image = "symptoma/activemq:5.16.3"
  }
  host         = "queue"
  link_credit  = 2
  max_priority = 10
  max_retries  = 10
  partition    = "TestPartition"
  password     = "admin"
  port         = 5672
  user         = "admin"
}

object_storage = {
  name  = "local"
  image = ""
}

logging_env_vars = {
  aspnet_core_env = "Development"
  log_level       = "Information"
}

submitter = {
  image = "dockerhubaneo/armonik_control"
  name  = "armonik.control.submitter"
  port  = 5001
}

compute_plane = {
  polling_agent = {
    image                = "dockerhubaneo/armonik_pollingagent"
    max_error_allowed    = -1
    name                 = "armonik.compute.pollingagent"
    port                 = 9980
    worker_check_delay   = "00:00:10"
    worker_check_retries = 10
  }
  worker = {
    image                    = "dockerhubaneo/armonik_core_htcmock_test_worker"
    name                     = "armonik.compute.worker"
    port                     = 1080
    serilog_application_name = "ArmoniK.Compute.Worker"
  }
}

armonik_metrics_image = "dockerhubaneo/armonik_control_metrics"

armonik_partition_metrics_image = "dockerhubaneo/armonik_control_partition_metrics"

log_driver_image = "fluent/fluent-bit:latest"

seq_image = "datalust/seq:latest"

zipkin_image = "openzipkin/zipkin:latest"

database_image = "mongo"
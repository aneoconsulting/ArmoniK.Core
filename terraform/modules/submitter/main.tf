resource "docker_image" "submitter" {
  count        = var.use_local_image ? 0 : 1
  name         = "${var.docker_image}:${var.core_tag}"
  keep_locally = true
}

module "submitter_local" {
  count           = var.use_local_image ? 1 : 0
  source          = "../build_image"
  use_local_image = var.use_local_image
  image_name      = "submitter_local"
  context_path    = "${path.root}../"
  dockerfile_path = "${path.root}../Control/Submitter/src/"
}

resource "docker_container" "submitter" {
  name  = var.container_name
  image = var.use_local_image ? module.submitter_local[0].image_id : docker_image.submitter[0].image_id

  networks_advanced {
    name = var.network
  }

  env = [
    "Components__TableStorage=ArmoniK.Adapters.MongoDB.TableStorage",
    "MongoDB__Host=database",
    "MongoDB__Port=27017",
    "MongoDB__DatabaseName=database",
    "MongoDB__MaxConnectionPoolSize=500",
    "MongoDB__TableStorage__PollingDelayMin=00:00:01",
    "MongoDB__TableStorage__PollingDelayMax=00:00:10",
    "Components__ObjectStorage=ArmoniK.Adapters.Redis.ObjectStorage",
    "Redis__EndpointUrl=object:6379",
    "Submitter__DefaultPartition=TestPartition0",
    "Serilog__MinimumLevel=Information",
    "Zipkin__Uri=${var.zipkin_uri}",
    "ASPNETCORE_ENVIRONMENT=Development",
    "Components__QueueStorage=ArmoniK.Adapters.Amqp.QueueStorage",
    "Amqp__User=admin",
    "Amqp__Password=admin",
    "Amqp__Host=queue",
    "Amqp__Port=5672",
    "Amqp__Scheme=AMQP",
    "Amqp__MaxPriority=10",
    "Amqp__MaxRetries=10",
    "Amqp__LinkCredit=2"
  ]

  log_driver = var.log_driver.name

  log_opts = {
    fluentd-address = var.log_driver.address
  }

  ports {
    internal = 1080
    external = 5001
  }

  ports {
    internal = 1081
    external = 5011
  }
}
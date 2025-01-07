locals {
  
  is_github_actions = can(regex("/home/runner/work/", abspath("${path.module}")))

  effective_certs_path = var.object_storage.name == "redis" ? (
    local.is_github_actions 
      ? replace(abspath("${path.module}/../../modules/storage/object/redis/generated/redis/certs"), "/ArmoniK.Core/ArmoniK.Core/", "ArmoniK.Core/ArmoniK.Core")
      : abspath("${path.module}/../storage/object/redis/generated/redis/certs")
  ) : null

  redis_cert_mounts = var.object_storage.name == "redis" ? {
    "${local.effective_certs_path}/ca.pem"    = "/redis/certs/ca.pem",
    "${local.effective_certs_path}/redis.crt" = "/redis/certs/redis.crt",
    "${local.effective_certs_path}/redis.key" = "/redis/certs/redis.key"
  } : {}

  env = [
    "Submitter__DefaultPartition=TestPartition0"
  ] 

  gen_env = [for k, v in var.generated_env_vars : "${k}=${v}"]
}

locals {
  env     = [for k, v in var.metrics_env_vars : "${k}=${v}"]
  gen_env = [for k, v in var.generated_env_vars : "${k}=${v}"]
}
locals {
  gen_env = [for k, v in var.generated_env_vars : "${k}=${v}"]
}
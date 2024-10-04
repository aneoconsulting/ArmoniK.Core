locals {
  init_env = [
    "InitServices__InitDatabase=${!var.container_init}",
  ]
  gen_env = [for k, v in var.generated_env_vars : "${k}=${v}"]
}

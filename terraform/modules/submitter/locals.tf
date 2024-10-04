locals {
  common_env = [
    "Submitter__DefaultPartition=TestPartition0",
  ]
  control_plane_env = [
    "InitServices__InitDatabase=${!var.container_init}",
  ]
  init_env = [
    "InitServices__StopAfterInit=true",
    "Serilog__Properties__Application=ArmoniK.Control.Init",
  ]
  gen_env = [for k, v in var.generated_env_vars : "${k}=${v}"]
}

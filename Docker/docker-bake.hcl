# Core images (PollingAgent, Submitter, Metrics), built together with `just build-core`.
# Building them in a single bake invocation shares the build stage of the Dockerfile
# and exports/pushes the three images in parallel.
# Image names are read from the environment variables exported by the justfile.
# Relative paths (context, dockerfile) are resolved from the directory bake runs in: the repository root.

variable "ARMONIK_METRICS" {
  default = "dockerhubaneo/armonik_control_metrics:0.0.0.0-local"
}

variable "ARMONIK_SUBMITTER" {
  default = "dockerhubaneo/armonik_control:0.0.0.0-local"
}

variable "ARMONIK_POLLINGAGENT" {
  default = "dockerhubaneo/armonik_pollingagent:0.0.0.0-local"
}

group "default" {
  targets = ["core"]
}

group "core" {
  targets = ["metrics", "submitter", "polling_agent"]
}

target "_core" {
  context    = "."
  dockerfile = "Dockerfile"
}

target "metrics" {
  inherits = ["_core"]
  target   = "metrics"
  tags     = [ARMONIK_METRICS]
}

target "submitter" {
  inherits = ["_core"]
  target   = "submitter"
  tags     = [ARMONIK_SUBMITTER]
}

target "polling_agent" {
  inherits = ["_core"]
  target   = "polling_agent"
  tags     = [ARMONIK_POLLINGAGENT]
}

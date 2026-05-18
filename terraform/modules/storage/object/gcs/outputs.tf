output "generated_env_vars" {
  value = ({
    "Components__ObjectStorageAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.Gcs.ObjectBuilder"
    "Components__ObjectStorageAdaptorSettings__AdapterAbsolutePath" = "/adapters/object/gcs/ArmoniK.Core.Adapters.Gcs.dll"
    "Gcs__ProjectId"                                                = var.project_id
    "Gcs__BucketName"                                               = var.bucket_name
    "Gcs__EmulatorEndpoint"                                         = "http://${var.host}:${var.port}/storage/v1/"
  })
}

output "volumes" {
  description = "Volumes that agents and submitters must mount to access the object storage"
  value       = {}
}

output "core_mounts" {
  description = "Volumes that agents and submitters must mount to access the object storage"
  value       = {}
}

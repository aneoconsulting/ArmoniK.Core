output "generated_env_vars" {
  value = ({
    "Components__ObjectStorageAdaptorSettings__ClassName"           = "ArmoniK.Core.Adapters.S3.ObjectBuilder"
    "Components__ObjectStorageAdaptorSettings__AdapterAbsolutePath" = "/adapters/object/s3/ArmoniK.Core.Adapters.S3.dll"
    "S3__EndpointUrl"                                               = "http://${var.host}:${var.port}"
    "S3__BucketName"                                                = var.bucket_name
    "S3__Login"                                                     = var.login
    "S3__Password"                                                  = var.password
    "S3__MustForcePathStyle"                                        = true
  })
}

output "volumes" {
  description = "Volumes that agents and submitters must mount to access the object storage"
  value       = {}
}

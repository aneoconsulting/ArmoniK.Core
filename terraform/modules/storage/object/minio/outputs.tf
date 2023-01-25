output "generated_env_vars" {
  value = ({
    "Components__ObjectStorage" = "ArmoniK.Adapters.S3.ObjectStorage",
    "S3__EndpointUrl"       = "http://${var.minio_parameters.host}:${var.minio_parameters.port}"
    "S3__BucketName"    = var.minio_parameters.bucket_name
    "S3__Login" = var.minio_parameters.login 
    "S3__Password" = var.minio_parameters.password
    "S3__MustForcePathStyle" = true
  })
}
output "generated_env_vars" {
  value = ({
    "Components__ObjectStorage" = "ArmoniK.Adapters.S3.ObjectStorage",
    "S3__EndpointUrl"           = "http://${var.host}:${var.port}"
    "S3__BucketName"            = var.bucket_name
    "S3__Login"                 = var.login
    "S3__Password"              = var.password
    "S3__MustForcePathStyle"    = true
  })
}
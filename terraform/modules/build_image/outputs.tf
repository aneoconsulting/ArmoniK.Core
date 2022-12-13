output "image_id" {
  value = var.use_local_image ? "${var.image_name}:${null_resource.build_local_image[0].id}" : ""
}
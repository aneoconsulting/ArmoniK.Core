resource "null_resource" "build_local_image" {
  count = var.use_local_image ? 1 : 0

  triggers = {
    always_run = sha1(join("", [for f in fileset(path.module, "${var.dockerfile_path}/*") : filesha1(f)]))
  }

  provisioner "local-exec" {
    command     = "docker build -t \"${var.image_name}:\"${null_resource.build_local_image[0].id} -f \"${var.dockerfile_path}Dockerfile\" ${var.context_path}"
    interpreter = ["/bin/sh", "-c"]
  }
}

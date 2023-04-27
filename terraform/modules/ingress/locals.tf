locals {
  volumes_list = concat([
    {
      target = "/etc/nginx/conf.d/default.conf"
      source = abspath(local_file.ingress_conf_file.filename)
    }
    ],
    var.tls ? [{
      target = "/ingress.crt"
      source = abspath("${path.root}/.terraform/${var.container_name}/server/ingress.crt")
      },
      {
        target = "/ingress.key"
        source = abspath("${path.root}/.terraform/${var.container_name}/server/ingress.key")
    }] : [],
    var.mtls ? [{
      target = "/client_ca.crt"
      source = abspath("${path.root}/.terraform/${var.container_name}/client/client_ca.crt")
    }] : []
  )
  volume_map = tomap({
    for t in local.volumes_list : t.target => t
  })
}
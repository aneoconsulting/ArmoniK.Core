locals {
  volumes_list = concat([
    {
      target = "/etc/nginx/conf.d/default.conf"
      source = abspath(local_file.ingress_conf_file.filename)
    }
    ],
    var.tls ? [{
      target = "/ingress"
      source = abspath("${path.root}/.terraform/${var.container_name}/server/")
    }] : [],
    var.mtls ? [{
      target = "/ingressclient"
      source = abspath("${path.root}/.terraform/${var.container_name}/client/")
    }] : []
  )
  volume_map = tomap({
    for t in local.volumes_list : t.target => t
  })
}
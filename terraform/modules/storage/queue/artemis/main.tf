resource "docker_image" "queue" {
  name         = var.image
  keep_locally = true
}

resource "docker_container" "queue" {
  name  = "queue"
  image = docker_image.queue.image_id

  networks_advanced {
    name = var.network
  }

  env = [
    "AMQ_EXTRA_ARGS=--relax-jolokia --http-host 0.0.0.0",
    "AMQ_USER=admin",
    "AMQ_PASSWORD=admin",
    "container=oci",
    "HOME=/home/jboss",
    "JAVA_HOME=/usr/lib/jvm/java-17",
    "JAVA_VENDOR=openjdk",
    "JAVA_VERSION=17",
    "JBOSS_CONTAINER_OPENJDK_JDK_MODULE=/opt/jboss/container/openjdk/jdk",
    "AB_PROMETHEUS_JMX_EXPORTER_CONFIG=/opt/jboss/container/prometheus/etc/jmx-exporter-config.yaml",
    "JBOSS_CONTAINER_PROMETHEUS_MODULE=/opt/jboss/container/prometheus",
    "JBOSS_CONTAINER_MAVEN_36_MODULE=/opt/jboss/container/maven/36/",
    "MAVEN_VERSION=3.6",
    "S2I_SOURCE_DEPLOYMENTS_FILTER=*.jar",
    "JBOSS_CONTAINER_S2I_CORE_MODULE=/opt/jboss/container/s2i/core/",
    "JBOSS_CONTAINER_JAVA_PROXY_MODULE=/opt/jboss/container/java/proxy",
    "JBOSS_CONTAINER_JAVA_JVM_MODULE=/opt/jboss/container/java/jvm",
    "JBOSS_CONTAINER_UTIL_LOGGING_MODULE=/opt/jboss/container/util/logging/",
    "JBOSS_CONTAINER_MAVEN_DEFAULT_MODULE=/opt/jboss/container/maven/default/",
    "JBOSS_CONTAINER_MAVEN_S2I_MODULE=/opt/jboss/container/maven/s2i",
    "JAVA_DATA_DIR=/deployments/data",
    "JBOSS_CONTAINER_JAVA_RUN_MODULE=/opt/jboss/container/java/run",
    "JBOSS_CONTAINER_JAVA_S2I_MODULE=/opt/jboss/container/java/s2i",
    "JBOSS_IMAGE_NAME=ubi8/openjdk-17",
    "JBOSS_IMAGE_VERSION=1.14",
    "LANG=C.utf8",
    "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/s2i",
    "APACHE_ARTEMIS_VERSION=2.28.0",
    "AMQ_CLUSTER_PASSWORD=cluster_password",
    "AMQ_CLUSTER_USER=cluster_user",
    "AMQ_CLUSTERED=false",
    "AMQ_HOME=/opt/amq",
    "AMQ_NAME=broker",
    "AMQ_RESET_CONFIG=false",
    "AMQ_ROLE=admin"
  ]

  ports {
    internal = 5672
    external = var.exposed_ports.amqp_connector
  }

  ports {
    internal = 8161
    external = var.exposed_ports.admin_interface
  }
}
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-green.svg)](https://www.gnu.org/licenses/agpl-3.0)


# Project components

This page intends to describe the different software projetcs and components used for ArmoniK 
internals. To learn how to install or use ArmoniK, please see these repositories:
  * install: 
  * use: 


## Common Library

**ArmoniK.Common** provides the components required by all the other compononents of ArmoniK.

## Control

Different projects are available in the control plan:

* **ArmoniK.Control.Submitter** is a web server providing the gRPC services required by the 
SubmitterService client from SDK. It provides the following services:
  * Session creation
  * Task submission
  * Results retrieving
  * Task cancellation
  * Session cancellation

* **ArmoniK.Control.Monitor (not yet available)** is a web server providing the gRPC services 
required by the MonitorService client from SDK. It provides the followin services:
  * List all session and tasks
  * Count the session and task per status
  * Get the execution details for the tasks

* **ArmoniK.Control.ResourceManager (not yet available)** is a web server providing gRPC 
services required by the ResourceManager client from SDK. It provides the following services:
  * Upload new resources
  * Download new resources
  * List all available resources
  * Delete resources

* **ArmoniK.Control.Autoscaling (not yet available)** is a process that regularly computes 
metrics used to determine the number of POD instances required on the grid. This will then 
be used by Kubernetes to start/stop compute nodes (on elastic configurations such as managed 
K8s service on cloud).

## Compute Plan Components

From a software enginnering point of view, the compute plan relies on three components:

* **ArmoniK.Compute.PollingAgent** implements a ArmoniK.DevelopmentKit.Gridlib.GRPC client.
It acts as a proxy between the gridlib agent container and the rest of the ArmoniK system. 
Using such a proxy agent allows all the ArmoniK logic to be implemented independantly of 
the Gridlib agent. Hence handling an new agent to handle new languages will be easier.

* Other companion containers for the gridlib agent container Such cantainers could provide 
the following services : 
  * setting up secrets for the POD
  * forward the logs to a log cypher (ex: ELK or a cloud equivalent)
  * host a cache on each node used (required a deamon set to be really efficient)

## Adaptors

The adaptors projects provide implementations of the interfaces relying on different kind of 
services. We plan to provide an implementation for on-premises installation and each cloud provider.

The following adaptors are currently available:
  * ArmoniK.Adaptors.MongoDB provides all components to run ArmoniK. A queue implementation is even 
    available although it is not as efficient and scalable as dedicated products.
  * ArmoniK.Adaptors.Amqp provides an AMQP connector for ArmoniK. It allows using any AMQP server
    such as ActiveMQ, ActiveMQ Artemis, RabbitMQ or IBM MQ

The following adaptors will be available soon:
  * ArmoniK.Adaptors.Redis to provide a RedisCache implementation for the internal storage.
  * ArmoniK.Adaptors.S3 to provide a S3 implementation for the internal storage.
  * ArmoniK.Adaptors.AmazonSQS to provide a SQS implementation for the internal queue.
  * ArmoniK.Adaptors.DynamoDB to provide a DynamoDB implementation for the internal state database
  
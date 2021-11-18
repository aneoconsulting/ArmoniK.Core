# ArmoniK.Adaptors

Here are the adapters to allow ArmoniK to rely on different 
services corresponding to the specific deployment choices.

Here are the default deployment configuration:
* __On-premises__: _Not integrated yet_
  * State DataBase = MongoDB
  * Controle plan host = Kubernetes
  * Queue = Redis MQ
  * Internal Data Storage = Redis
  * Log Ingestion = Not defines, use plain K8s

* __AWS hybrid__: _Not integrated yet_
  * State DataBase = DynamoDB
  * Controle plan host = EKS
  * Queue = SQS
  * Internal Data Storage = Redis
  * Log Ingestion = CloudWatch

* __GCP__: To be defined

* __Azure__: To be defined
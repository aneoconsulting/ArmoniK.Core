# ArmoniK.Adaptors

Here are the adapters to allow ArmoniK to rely on different 
services corresponding to the specific deployment choices.

## Queue Adapters

ArmoniK provides multiple queue adapter implementations:

- **AMQP (Generic)** - `ArmoniK.Core.Adapters.Amqp`
  - Supports any AMQP 1.0 compatible server (ActiveMQ Artemis, IBM MQ, etc.)

- **RabbitMQ** - `ArmoniK.Core.Adapters.RabbitMQ`
  - Dedicated RabbitMQ adapter using AMQP 0.9.1 protocol
  - Enhanced priority support

- **Amazon SQS** - `ArmoniK.Core.Adapters.SQS`
  - AWS Simple Queue Service integration

- **Google Cloud Pub/Sub** - `ArmoniK.Core.Adapters.PubSub`
  - Google Cloud messaging service integration

- **Nats Jet Stream** - `ArmoniK.Core.Adapters.Nats`
  - Messaging service integration

- **Memory** - `ArmoniK.Core.Adapters.Memory`
  - In-memory queue for testing and development

## Object Storage Adapters

- **Amazon S3** - `ArmoniK.Core.Adapters.S3`
  - AWS S3 compatible object storage

- **Local Storage** - `ArmoniK.Core.Adapters.LocalStorage`
  - File system-based storage for development and testing

- **Redis** - `ArmoniK.Core.Adapters.Redis`
  - Redis-based object storage with chunking

## Database Adapters

- **MongoDB** - `ArmoniK.Core.Adapters.MongoDB`
  - Provides table storage for tasks, sessions, results, and partitions

- **Memory** - `ArmoniK.Core.Adapters.Memory`
  - In-memory tables for testing and development

## Dynamic Loading

Adapters are loaded dynamically at startup through the `IDependencyInjectionBuildable` interface. Each adapter implements a `Build` method that registers its services through dependency injection.

Configuration example:
```json
{
  "Components": {
    "QueueAdaptorSettings": {
      "ClassName": "ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder",
      "AdapterAbsolutePath": "/path/to/adapter.dll"
    }
  }
}
```

## Deployment Examples

### On-premises
- **Queue**: RabbitMQ or AMQP or Nats
- **Object Storage**: Minio
- **Database**: MongoDB

### AWS
- **Queue**: Amazon SQS
- **Object Storage**: Amazon S3
- **Database**: MongoDB

### Google Cloud
- **Queue**: Google Cloud Pub/Sub
- **Object Storage**: GCS
- **Database**: MongoDB

### Development/Testing
- **Queue**: Memory
- **Object Storage**: Embedded or Local Storage
- **Database**: Memory





# ArmoniK.Adaptors

Here are the adapters to allow ArmoniK to rely on different 
services corresponding to the specific deployment choices.

## Queue Adapters

ArmoniK provides multiple queue adapter implementations:

- **AMQP (Generic)** - `ArmoniK.Core.Adapters.Amqp`
  - Supports any AMQP 1.0 compatible server (ActiveMQ Artemis, IBM MQ, etc.)
  - Status: GA

- **RabbitMQ** - `ArmoniK.Core.Adapters.RabbitMQ`
  - Dedicated RabbitMQ adapter using AMQP 0.9.1 protocol
  - Enhanced priority support
  - Status: GA

- **Amazon SQS** - `ArmoniK.Core.Adapters.SQS`
  - AWS Simple Queue Service integration
  - Status: GA

- **Google Cloud Pub/Sub** - `ArmoniK.Core.Adapters.PubSub`
  - Google Cloud messaging service integration
  - Status: GA

- **Memory** - `ArmoniK.Core.Adapters.Memory`
  - In-memory queue for testing and development
  - Status: GA

## Object Storage Adapters

- **MongoDB GridFS** - `ArmoniK.Adapters.MongoDB.ObjectStorage`
- **Amazon S3** - `ArmoniK.Core.Adapters.S3`
- **Local Storage** - `ArmoniK.Core.Adapters.LocalStorage`
- **Redis** - `ArmoniK.Core.Adapters.Redis`
- **Embedded Storage** - `ArmoniK.Core.Adapters.Embed`

## Database Adapters

- **MongoDB** - `ArmoniK.Adapters.MongoDB.TableStorage`
  - Task metadata and session storage

- **MongoDB Authentication** - `ArmoniK.Adapters.MongoDB.AuthenticationTable`
  - User authentication and authorization storage

## Dynamic Loading

Adapters are loaded dynamically at startup through the `IDependencyInjectionBuildable` interface. Each adapter implements a `Build` method that registers its services through dependency injection.

Configuration is done through the `Components` section:
```json
{
  "Components": {
    "QueueAdaptorSettings": {
      "ClassName": "ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder",
      "AdapterAbsolutePath": "/path/to/adapter.dll"
    },
    "TableStorage": "ArmoniK.Adapters.MongoDB.TableStorage",
    "ObjectStorage": "ArmoniK.Adapters.MongoDB.ObjectStorage",
    "AuthenticationStorage": "ArmoniK.Adapters.MongoDB.AuthenticationTable"
  }
}
```

## Deployment Examples

### On-premises (Kubernetes)
- **Queue**: RabbitMQ or AMQP
- **Object Storage**: MongoDB GridFS or Local Storage
- **Database**: MongoDB

### AWS (EKS)
- **Queue**: Amazon SQS
- **Object Storage**: Amazon S3
- **Database**: MongoDB

### Google Cloud (GKE)
- **Queue**: Google Cloud Pub/Sub
- **Object Storage**: Google Cloud Storage
- **Database**: MongoDB

### Development/Testing
- **Queue**: Memory or RabbitMQ
- **Object Storage**: Local Storage or Embedded
- **Database**: MongoDB


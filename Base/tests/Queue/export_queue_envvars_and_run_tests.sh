#!/bin/bash
# Scripts needs to be launched with protocol=rabbitmq or protocol=amqp1_0 as parameters and source it
if [ -z "$protocol" ]; then
  echo "The protocol variable is not set. Use 'amqp1_0' or 'rabbitmq'."
  exit 1
fi

if [ "$protocol" = "amqp1_0" ]; then
  export Components__QueueAdaptorSettings__ClassName="ArmoniK.Core.Adapters.Amqp.QueueBuilder"
  export Components__QueueAdaptorSettings__AdapterAbsolutePath="/home/nicodl/code/ArmoniK.Core/Adaptors/Amqp/src/bin/Debug/net8.0/ArmoniK.Core.Adapters.Amqp.dll"
elif [ "$protocol" = "rabbitmq" ]; then
  export Components__QueueAdaptorSettings__ClassName="ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder"
  export Components__QueueAdaptorSettings__AdapterAbsolutePath="/home/nicodl/code/ArmoniK.Core/Adaptors/RabbitMQ/src/bin/Debug/net8.0/ArmoniK.Core.Adapters.RabbitMQ.dll"
else
  echo "Invalid value for protocol. Use 'amqp1_0' or 'rabbitmq'."
  exit 1
fi

export Amqp__User="guest" 
export Amqp__Password="guest"
export Amqp__Host="localhost"
export Amqp__Port="5672"
export Amqp__Scheme="amqp"
export Amqp__PartitionId="TestPartition"
export Amqp__MaxPriority="10"
export Amqp__MaxRetries="5"
export Amqp__AllowHostMismatch="false"
export Amqp__LinkCredit="1"
export RabbitMQ__User="guest"
export RabbitMQ__Password="guest"
export RabbitMQ__Host="localhost"
export RabbitMQ__Port="5672"
export RabbitMQ__Scheme="amqp"
export RabbitMQ__PartitionId="TestPartition"
export RabbitMQ__MaxPriority="10"
export RabbitMQ__MaxRetries="10"
export RabbitMQ__AllowHostMismatch="false"
export RabbitMQ__LinkCredit="2"

echo "Components__QueueAdaptorSettings__ClassName: $Components__QueueAdaptorSettings__ClassName"
echo "Components__QueueAdaptorSettings__AdapterAbsolutePath: $Components__QueueAdaptorSettings__AdapterAbsolutePath"

dotnet test --logger "trx;LogFileName=test-results.trx" -p:RunAnalyzers=false -p:WarningLevel=0
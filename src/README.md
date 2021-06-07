# Project components

This page intends to describe the different projetcs and components used for ArmoniK.


## Development Kit

The SDK provides classes to be used by application integrating ArmoniK. These classes are 
gathered three components:

* **ArmoniK.DevelopmentKit.Client** provides the classes required to open an ArmoniK session,
submit tasks and retrieve the results. 

* **ArmoniK.DevelopmentKit.GridLibAdapter** provides the classes required to integrate a gridlib 
* with ArmoniK.

* **ArmoniK.DevelopmentKit.Common** provides different classes used in both the SDKs. 

Currently, only .Net Core SDKs are provided. In order to allow the easy development of SDKs for 
other languages, the SDK is designed to be as lightweith as possible and to rely on gRPC. Hence, 
a very basic SDK can easily be generated from the gRPC *.proto files. We would still recommend to 
develop a higher level API to be easily used by the client software.


## Compute

From a software enginnering point of view, the compute plan relies on three components:

* **ArmoniK.Compute.GridLibAgent** is the agent in charge of hosting the gridlib. It relies on 
ASP.NET Core 5.0. It is in charge of providing different services to the gridlib such as loggers 
or exception management.

* **ArmoniK.Compute.PollingAgent** is an agent acting as a proxy between 
ArmoniK.Compute.GridLibAgent and the rest of the ArmoniK system. Using such a proxy agent allows 
all the ArmoniK logic to be implemented independantly of the Gridlib agent. Hence handling an new 
agent to handle new languages will be easier.

* **ArmoniK.Compute.gRPC** provides the *.proto files required to generate the communication 
channel between ArmoniK.Compute.GridLibAgent and ArmoniK.Compute.PollingAgent. gRPC can be used 
to generate interfaces in many languages. Thus, implementing a new gridlib adapter should be 
quite easy.

## Control (Not available yet)

Different projects are available in the control plan:

* **ArmoniK.Control.Server** is a web server providing the gRPC services required by the client 
SDK. It should provide the following services:
  * Rights management
  * Job creation
  * Task submission
  * Task monitoring
  * Results retrieving
  * Task cancellation
  * Job cancellation
  * Offloading to another ArmoniK grid (planned for ArmoniK 2.0)

* **ArmoniK.Control.TaskRetryCheck** is a process that regularly checks the states of the tasks
and their dispatches. When a heartbeat fails or when an execution finished with a failure, this 
process ensures that the task is resubmitted or put in the dead letters queue according to a 
given policy.

* **ArmoniK.Control.Autoscaling** is a process that regularly computes metrics used to determine
the number of POD instances required on the grid. This will then be used by Kubernetes to 
start/stop compute nodes (on elastic configurations such as managed K8s service on cloud).

## Common Library

**ArmoniK.Common** provides the components required by all the other compononents of ArmoniK.

## Adaptors

The adaptors projects provide implementations of the interfaces relying on different kind of 
services. We plan to provide an implementation for on-premises installation and one for each 
cloud provider.

# FAQ

## Can ArmoniK handle multigrid?

In the first version, this feature will not be directly available. However, the client libraries 
can be configured to connect to different ArmoniK grids ths providing an equivalent 
functionnality. A more transparent approach is planned for the version 2.0 of ArmoniK.

## Can ArmoniK handle dynamic offloading/loadbalancing between grids?

This feature is planned for the 2.0 version. The approach planed is to have a main *parent* 
grid (usually corresponding to a on-premises grid). When submitting tasks to this grid, one will 
be able either to directly provide on which grid the task must be run or to let the system 
decide. In the later case, depending on offloading rules, the task will either run on this 
*parent* grid or on one of the *children* grids. However, in case of tasks submitting other tasks 
to the grid (i.e. the gridlib uses the client SDK), no mechanism will be provided by ArmoniK to 
handle requeuing these  tasks from a *child* grid to the *parent* grid or to another *child* 
grid. To handle such case, the gridlib would need to be configured to submit the tasks directly 
to the *parent* grid.

In a future version, a complementary component could be add to transfer tasks between the 
different ArmoniK grids.

## Can ArmoniK provide a functionnality equivelent to the Tibco Datasynapse DDT?

While providing such a functionality is not planned at this stage, this can be implemented 
relatively easily. The principle of the solution would be to add a new gRPC server inthe client 
SDK. The ArmoniK.Compute.PollingAgent can then send the result directly to this gRPC server. Such 
a feature would not change the tasks throughput but would reduce de elapsed time required for a 
client to get the results from the tasks it submitted.

## How are Exceptions from gridlib handled?

Exception that are not handled from within the gridlib are caught by ArmoniK.Compute.GridLibAgent. 
The dispatch is then marked as failed and the task will be analyzed to decide whether it require 
resubmission. This analysis is performed by ArmoniK.Control.TaskRetryCheck.

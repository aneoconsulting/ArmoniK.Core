# Project components

This page intends to describe the different software projetcs and components used for ArmoniK.
It does not present the infrastructure componenents and how they communicate with each other.

## Development Kit

Currently, only .Net SDKs are provided. In order to allow the easy development of SDKs for 
other languages, the low level SDK is designed to be as lightweith as possible and generated 
directly from the gRPC *.proto files. These files are provided in the 
ArmoniK.DevelopmentKit.ClientGRPC and ArmoniK.DevelopmentKit.GridlibGRPC projects.

When using ArmoniK with another language, we recommend the implementation of a higher level
abstraction than the low level gRPC API. Example of such APIs can be found in the 
ArmoniK.DevelopmentKit.Client and ArmoniK.DevelopmentKit.GridLibAdapter projects.

Here is a description of the different projects provided as part of the SDK.

* **ArmoniK.DevelopmentKit.Client.GRPC** provides the *.proto client SDK files. They can be 
used to compile a low level SDK for any language having a gRPC compiler (see [the list of
languages on the grpc documentation page](https://grpc.io/docs/languages/)).

* **ArmoniK.DevelopmentKit.Client.LowLevelAPI** provides the low level .Net API generated from 
the *.proto client SDK files.

* **ArmoniK.DevelopmentKit.Client.HighLevelAPI** provides a higher level API than the 
LowLevelAPI project. We recommend using this project when building a .Net
application. One could inspire from this project to build high level SDKs in other languages.

* **ArmoniK.DevelopmentKit.Gridlib.GRPC** provides the *.proto gridlib SDK files. They can be 
used to compile a low level SDK for any language having a gRPC compiler (see [the list of
languages on the grpc documentation page](https://grpc.io/docs/languages/)). To 
integrate a gridlib, one has to run a GridlibGRPC service that will be called by the 
ArmoniK.Compute.PollingAgent. To implement new server in the supported languages, one can follow 
[the gRPC documentation](https://grpc.io/docs/)

* **ArmoniK.DevelopmentKit.GridLibService.LowLevelAPI** provides the low level .Net API 
generated from the *.proto gridlib SDK files. To use this API, one should host the provided 
service in a webserver. 

* **ArmoniK.DevelopmentKit.GridLib.GridLibAgent** provides a higher level API than the
LowLevelAPI project. We recommend using this project when building a .Net application. One 
could inspire from this project to build high level SDKs in other languages. This adapter 
relies on the asp.Net core 5.0 server. 


## Compute Plan Components

From a software enginnering point of view, the compute plan relies on three components:

* **ArmoniK.Compute.PollingAgent** implements a ArmoniK.DevelopmentKit.Gridlib.GRPC client.
It acts as a proxy between the gridlib agent container and the rest of the ArmoniK system. 
Using such a proxy agent allows all the ArmoniK logic to be implemented independantly of 
the Gridlib agent. Hence handling an new agent to handle new languages will be easier.

* Other companion containers for the gridlib agent container ? Such cantainers could provide 
the following services : 
  * setting up secrets for the POD
  * forward the logs to a log cypher (ex: ELK or a cloud equivalent)

## Control (Not available yet)

Different projects are available in the control plan:

* **ArmoniK.Control.Server** is a web server providing the gRPC services required by the client 
SDK. It should provide the following services:
  * Rights management
  * Session creation
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

**ArmoniK.Common.GRPCAdapters** provides extensions to convert between the ArmoniK.Common data
model and the developmentKit gRPC data models.

**ArmoniK.Common.Adaptors** provides the interfaces and utilities required to make and use the 
different storage backends.

## Adaptors

The adaptors projects provide implementations of the interfaces relying on different kind of 
services. We plan to provide an implementation for on-premises installation and one for each 
cloud provider.

# FAQ

### Can ArmoniK handle multigrid?

In the first version, this feature will not be directly available. However, the client libraries 
can be configured to connect to different ArmoniK grids ths providing an equivalent 
functionnality. A more transparent approach is planned for the version 2.0 of ArmoniK.

### Can ArmoniK handle dynamic offloading/loadbalancing between grids?

This feature is planned for the 2.0 version. The approach planed is to have a main *parent* 
grid (usually corresponding to a on-premises grid). When submitting tasks to this grid, one will 
be able either to directly provide on which grid the task must be run or to let the system 
decide. In the later case, depending on offloading rules, the task will either run on this 
*parent* grid or on one of the *children* grids. However, in case of tasks submitting other tasks 
to the grid (i.e. the gridlib uses the client SDK), no mechanism will be provided by ArmoniK to 
handle requeuing these  tasks from a *child* grid to the *parent* grid or to another *child* 
grid. To handle such case, the gridlib would need to be configured to submit the tasks directly 
to the *parent* grid.

In a future version, a complementary component could be add to dynamically transfer tasks between 
the different ArmoniK grids.

### Does ArmoniK provide a functionnality equivelent to the Tibco Datasynapse DDT?

While providing such a functionality is not planned at this stage, this can be implemented 
relatively easily. The principle of the solution would be to add a gRPC server with the client 
SDK. The ArmoniK.Compute.PollingAgent can then communicate directly with this gRPC server. Such 
a feature would probably not change the tasks throughput but would reduce de elapsed time required for a 
client to get the results from the tasks it submitted.

### How are Exceptions from gridlib handled?

Exception that are not handled from within the gridlib are caught by ArmoniK.Compute.GridLibAgent. 
The dispatch is then marked as failed and the task will be analyzed to decide whether it require 
resubmission. This analysis is performed by ArmoniK.Control.TaskRetryCheck.

### How is performed the component injection within ArmoniK?

The answer to this question has not been decided yet.

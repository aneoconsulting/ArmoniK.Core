[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-green.svg)](https://www.gnu.org/licenses/agpl-3.0)

# ArmoniK.Core

| Stable                                                                                                                                                                                                                                                       | Edge                                                                                                                                                                                                                                                       |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [![Docker image latest version](https://img.shields.io/docker/v/dockerhubaneo/armonik_pollingagent?color=fe5001&label=armonik_pollingagent&sort=semver)](https://hub.docker.com/r/dockerhubaneo/armonik_pollingagent)                                        | [![Docker image latest version](https://img.shields.io/docker/v/dockerhubaneo/armonik_pollingagent?color=fe5001&label=armonik_pollingagent&sort=date)](https://hub.docker.com/r/dockerhubaneo/armonik_pollingagent)                                        |
| [![Docker image latest version](https://img.shields.io/docker/v/dockerhubaneo/armonik_control_metrics?color=fe5001&label=armonik_control_metrics&sort=semver)](https://hub.docker.com/r/dockerhubaneo/armonik_control_metrics)                               | [![Docker image latest version](https://img.shields.io/docker/v/dockerhubaneo/armonik_control_metrics?color=fe5001&label=armonik_control_metrics&sort=date)](https://hub.docker.com/r/dockerhubaneo/armonik_control_metrics)                               |
| [![Docker image latest version](https://img.shields.io/docker/v/dockerhubaneo/armonik_control_partition_metrics?color=fe5001&label=armonik_control_partition_metrics&sort=semver)](https://hub.docker.com/r/dockerhubaneo/armonik_control_partition_metrics) | [![Docker image latest version](https://img.shields.io/docker/v/dockerhubaneo/armonik_control_partition_metrics?color=fe5001&label=armonik_control_partition_metrics&sort=date)](https://hub.docker.com/r/dockerhubaneo/armonik_control_partition_metrics) |
| [![Docker image latest version](https://img.shields.io/docker/v/dockerhubaneo/armonik_control?color=fe5001&label=armonik_control&sort=semver)](https://hub.docker.com/r/dockerhubaneo/armonik_control)                                                       | [![Docker image latest version](https://img.shields.io/docker/v/dockerhubaneo/armonik_control?color=fe5001&label=armonik_control&sort=date)](https://hub.docker.com/r/dockerhubaneo/armonik_control)                                                       |


 ## What is ArmoniK.Core?

This project is part of the [ArmoniK](https://github.com/aneoconsulting/ArmoniK) project. ArmoniK.Core is responsible for the implementation of the services needed for ArmoniK which are defined in [ArmoniK.Api](https://github.com/aneoconsulting/ArmoniK.Api).

ArmoniK.Core provides services for submitting computational tasks, keeping track of the status of the tasks and retrieving the results of the computations. The tasks are processed by external workers whose interfaces are also defined in ArmoniK.Api. ArmoniK.Core sends tasks to the workers, manages eventual errors during the execution of the tasks and manages also the storage of the task's results.

More detailed information on the inner working of ArmoniK.Core is available [here](https://aneoconsulting.github.io/ArmoniK.Core/).

## Features availability

As this repository is open-source and we follow trunk based development, all features will be integrated into main, in multiple small steps.
Breaking changes will be limited as much as possible or gated behind feature flags.
If breaking changes should happen, they will be documented in the releases.
Therefore, features in development or testing that cannot fit within a branch and need to be integrated into main will be marked as preview so that users know that these features are still in development and subject to changes.

### General availability

This section will list the features and plugins that are generally available, highly curated and for which support is available.

- Queue plugins
  - RabbitMQ
  - AMQP
  - PubSub
- Storage plugins
  - Local
  - Redis
  - S3
- Database plugins
  - MongoDB

### Preview

This section will list features and plugins that are available on main but still in development. Their API should be stable enough for advanced users who want to test them before GA. Breaking changes will be notified in the release notes. Any feedback or issue encountered with these features are welcome !

- Queue plugins
  - SQS

### Beta (Internal preview)

This section will list features and plugins in active development. They are not stable enough and breaking changes may occur without any notice. Breaking changes will not always be documented. Before they are moved to the "Preview" stage, issues might be closed with the minimal messages "Under Active Development".

- Partitionning
  - Partition metrics exporter

## Installation

ArmoniK.Core can be installed only on Linux machines. For Windows users, it is possible to do it on [WSL2](https://learn.microsoft.com/en-us/windows/wsl/about).

### Prerequisites

- [Terraform](https://www.terraform.io/) version >= 1.4.2
- [Just](https://github.com/casey/just) >= 1.8.0
- [Dotnet](https://dotnet.microsoft.com/en-us/) >= 6.0
- [Docker](https://www.docker.com/) >= 20.10.16
- [GitHub CLI](https://cli.github.com/) >= 2.23.0 (optional)

### Local deployment

To deploy ArmoniK.Core locally, you need first to clone the repository [ArmoniK.Core](https://github.com/aneoconsulting/armonik.core). Then, to see all the available recipes for deployment, place yourself at the root of the repository ArmoniK.Core where the justfile is located, and type on your command line:

```shell
just
```

More about local deployment, see [Local Deployment of ArmoniK.Core](./.docs/content/0.installation/1.local-deployment.md).

### Tests

There are a number of tests that help to verify the successful installation of ArmoniK.Core. Some of them require a full deployment of ArmoniK.Core, for others a partial deployment is enough.

More about tests, see [Tests of ArmoniK.Core](./.docs/content/0.installation/2.tests.md).

## Contribution

Contributions are always welcome!

See [ArmoniK.Community](https://github.com/aneoconsulting/ArmoniK.Community) for ways to get started.

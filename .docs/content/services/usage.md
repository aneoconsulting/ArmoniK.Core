# How to use ArmoniK RPCs

## Client

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane

  c ->>+ cp: Create Session
  cp -->>- c: Session ID

  c ->>+ cp: Create result metadata (payload)
  cp -->>- c: Payload ID

  c ->>+ cp: Upload payload data
  cp -->>- c: #

  c ->>+ cp: Create result metadata (output)
  cp -->>- c: Output ID

  c ->>+ cp: Submit tasks
  cp -->>- c: Task ID

  c ->>+ cp: Wait for result availability (Events Service)
  cp -->>- c: Result availability notification

  c ->>+ cp: Retrieve result data
  cp -->>- c: #

  c ->>+ cp: Close Session
  cp -->>- c: #

  c ->>+ cp: Purge Data in Session
  cp -->>- c: #

  c ->>+ cp: Delete Session
  cp -->>- c: #
```

This diagram illustrates the typical interactions between a client and the control plane for managing sessions, submitting tasks, and retrieving results. It covers the lifecycle of a session, including creating, submitting, and closing tasks.

## Payload and metadata

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane

  c ->>+ cp: Create results (payload metadata+data)
  cp -->>- c: Payload ID

  c ->>+ cp: Create result metadata (output)
  cp -->>- c: Output ID

  c ->>+ cp: Submit tasks
  cp -->>- c: Task ID
```

This diagram demonstrates how a client creates payloads and metadata, submits tasks, and receives task metadatas. It focuses on the relationship between payloads, metadata, and task submission.

## Delayed payload upload

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane

  c ->>+ cp: Create result metadata (payload)
  cp -->>- c: Payload ID

  c ->>+ cp: Create result metadata (output)
  cp -->>- c: Output ID

  c ->>+ cp: Submit tasks
  cp -->>- c: Task ID

  c ->>+ cp: Upload payload data (task is queued here)
  cp -->>- c: #
```

This diagram explains the process of submitting tasks with delayed payload uploads. It shows how tasks are not queued until its dependencies including the payload are not ready.

## Import existing data from object storage

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane

  c ->>+ cp: Create result metadata (payload)
  cp -->>- c: Payload ID

  c ->>+ cp: Import exiting data for Payload ID
  cp -->>- c: #

  c ->>+ cp: Create result metadata (output)
  cp -->>- c: Output ID

  c ->>+ cp: Submit tasks
  cp -->>- c: Task ID
```

This diagram describes how a client can import existing data from object storage by associating it with a payload ID and submitting tasks based on that data.

## Agent/Worker

```mermaid
sequenceDiagram
  participant a as Agent
  participant w as Worker

  a ->>+ w: Send task

  w -->>+ a: Create results (metadata + data)
  a ->>- w: Results metadata

  w -->>+ a: Create results metadata (no data)
  a ->>- w: Results metadata

  w -->>+ a: Notify results data availability (result is stored on disk)
  a ->>- w: Results metadata

  w -->>+ a: Submit child tasks
  a ->>- w: Submitted tasks metadata

  w -->>- a: Task Output
```

This diagram outlines the interactions between an agent and a worker. It includes task distribution, result creation, child task submission, and result notifications.

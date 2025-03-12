# Sessions Service RPCs actions

For more information on session lifecycle, see our [AEP](https://github.com/aneoconsulting/ArmoniK.Community/blob/main/AEP/aep-00003.md) on the subject.

## Create Session

The following sequence diagram illustrates the internal interactions when creating sessions in the Sessions Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant p as PartitionsTable
  participant s as SessionsTable

  c ->>+ cp: Create Session
  cp ->>+ p: Check partitions given to the session exist
  p -->>- cp: #

  cp ->>+ s: Insert new sessions in database
  s -->>- cp: #

  cp -->>- c: Created tasks metadata
```

## Cancel Session

The following sequence diagram illustrates the internal interactions when cancelling a session in the Sessions Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant s as SessionsTable
  participant t as TasksTable
  participant r as ResultsTable

  c ->>+ cp: Cancel session

  cp ->>+ t: Set status of tasks related to the session to Cancelled
  t -->>- cp: #

  cp ->>+ r: Set status of results related to the session to Aborted
  r -->>- cp: #

  cp ->>+ s: Set session status to Cancelled
  s -->>- cp: #

  cp -->>- c: Updated session metadata
```

## Pause Session

The following sequence diagram illustrates the internal interactions when pausing sessions in the Sessions Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant s as SessionsTable
  participant t as TasksTable

  c ->>+ cp: Pause session
  cp ->>+ s: Set session status to Paused
  s -->>- cp: #

  cp ->>+ t: Set status of Submitted or Dispateched tasks related to the session to Paused
  t -->>- cp: #
  cp -->>- c: Updated session metadata
```

## Resume Session

The following sequence diagram illustrates the internal interactions when resuming sessions in the Sessions Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant s as SessionsTable
  participant t as TasksTable
  participant q as Queue

  c ->>+ cp: Resume session
  cp ->>+ s: Set session status to Running
  s -->>- cp: #

  cp ->>+ t: Set status of Paused tasks related to the session to Submitted by batch
  t -->>- cp: #

  cp ->>+ q: Insert tasks into the queue by batch
  q -->>- cp: #

  cp -->>- c: Updated session metadata
```

## Close Session

The following sequence diagram illustrates the internal interactions when closing sessions in the Sessions Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant s as SessionsTable

  c ->>+ cp: Close session
  cp ->>+ s: Set session status to Close
  s -->>- cp: #
  cp -->>- c: Updated session metadata
```

## Purge Session

The following sequence diagram illustrates the internal interactions when purging sessions in the Sessions Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant s as SessionsTable
  participant t as TasksTable
  participant r as ResultsTable
  participant o as ObjectStorage

  c ->>+ cp: Purge session
  cp ->>+ s: Get session metadata
  s -->>- cp: #

  cp ->>+ r: Get results related to the session
  r -->>- cp: #

  cp ->>+ o: Delete results
  o -->>- cp: #

  cp ->>+ r: Set results status to Deleted
  r -->>- cp: #

  cp ->>+ s: Update session status to Purged
  s -->>- cp: #

  cp -->>- c: Updated session metadata
```

## Delete Session

The following sequence diagram illustrates the internal interactions when deleting sessions in the Sessions Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant s as SessionsTable
  participant t as TasksTable
  participant r as ResultsTable

  c ->>+ cp: Delete session
  cp ->>+ s: Get session metadata
  s -->>- cp: #

  cp ->>+ t: Delete tasks metadata related to the session
  t -->>- cp: #

  cp ->>+ r: Delete results metadata related to the session
  r -->>- cp: #

  cp ->>+ s: Delete session metadata
  s -->>- cp: #

  cp -->>- c: Updated session metadata
```

## Stop submission

The following sequence diagram illustrates the internal interactions when stopping sessions in the Sessions Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant s as SessionsTable

  c ->>+ cp: Stop submission
  cp ->>+ s: Update session metadata
  s -->>- cp: #

  cp -->>- c: Updated session metadata
```

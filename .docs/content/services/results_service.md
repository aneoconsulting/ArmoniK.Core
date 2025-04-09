# Results Service RPCs actions

## Create Results Metadata

The following sequence diagram illustrates the internal interactions when creating results metadata in the Results Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant r as ResultsTable

  c ->>+ cp: Create Results
  cp ->>+ r: Insert Results Meta Data in Database
  r -->>- cp: #
  cp -->>- c: Created results metadata
```

## Create Results

The following sequence diagram illustrates the internal interactions when creating results metadata with their binary data in the Results Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant o as ObjectStorage
  participant r as ResultsTable

  c ->>+ cp: Create Results
  cp ->>+ o: Upload data
  o -->>- cp: Results Opaque (in the storage) ID

  cp ->>+ r: Insert Results Meta Data in Database
  r -->>- cp: #

  cp -->>- c: Created results metadata
```

## Upload Results Data

The following sequence diagram illustrates the internal interactions when uploading binary data associated to an already existing Result in the Results Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant s as SessionsTable
  participant o as ObjectStorage
  participant t as TasksTable
  participant r as ResultsTable
  participant q as Queue

  c ->>+ cp: Upload Results Data
  cp ->>+ s: Get session metadata
  s -->>- cp: #

  cp ->>+ o: Upload data
  o -->>- cp: Results Opaque (in the storage) ID

  cp ->>+ r: Complete result which data were uploaded
  r -->>- cp: #

  cp ->>+ r: Get tasks that depend on this result
  r -->>- cp: #

  cp ->>+ t: Remove this result from the tasks dependencies
  t -->>- cp: #

  cp ->>+ t: Get all tasks that depended on this result and has no more dependencies (ready tasks)
  t -->>- cp: #

  cp ->>+ q: Insert ready tasks into the Queue
  q -->>- cp: #

  cp ->>+ t: Set queued tasks status to Submitted
  t -->>- cp: #

  cp -->>- c: Updated result metadata
```

## Download Result Data

The following sequence diagram illustrates the internal interactions when downloading results data from the object storage in the Results Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant r as ResultsTable
  participant o as ObjectStorage

  c ->>+ cp: Download Result Data
  cp ->>+ r: Get result OpaqueID
  r -->>- cp: #

  cp ->>+ o: Initiate streamed download associated to OpaqueID
  o -->>- cp: #

  cp -->> c: Streamed result data
  o -->> cp: #
  cp -->> c: #
  o -->> cp: #
  cp -->>- c: #
```

## Import Results Data


The following sequence diagram illustrates the internal interactions when importing existing data into a Result in the Results Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant s as SessionsTable
  participant t as TasksTable
  participant r as ResultsTable
  participant q as Queue
  participant o as ObjectStorage

  c ->>+ cp: Import Result Data
  cp ->>+ s: Get session metadata
  s -->>- cp: #

  cp ->>+ o: Get sizes of the given OpaqueID the user want to associate to Results
  o -->>- cp: #

  cp ->>+ r: Get results to map
  r -->>- cp: #

  cp ->>+ r: Complete result which data were imported successfully
  r -->>- cp: #

  cp ->>+ r: Get tasks that depend on this result
  r -->>- cp: #

  cp ->>+ t: Remove this result from the tasks dependencies
  t -->>- cp: #

  cp ->>+ t: Get all tasks that depended on this result and has no more dependencies (ready tasks)
  t -->>- cp: #

  cp ->>+ q: Insert ready tasks into the Queue
  q -->>- cp: #

  cp ->>+ t: Set queued tasks status to Submitted
  t -->>- cp: #

  cp -->>- c: Updated result metadata
```


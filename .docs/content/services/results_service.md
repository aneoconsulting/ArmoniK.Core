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
  cp ->>+ o: Get session metadata
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

  cp ->>+ o: Get session metadata
  o -->>- cp: Results Opaque (in the storage) ID

  cp ->>+ r: Complete result which data were uploaded
  r -->>- cp: #

  cp ->>+ t: Get tasks whose dependencies are completed by this result
  t -->>- cp: #

  cp ->>+ t: Set all tasks status to Pending
  t -->>- cp: #

  cp ->>+ q: Insert ready tasks into the Queue
  q -->>- cp: #

  cp ->>+ t: Set queued tasks status to Submitted
  t -->>- cp: #

  cp -->>- c: Updated result metadata
```

## Download Results Data

The following sequence diagram illustrates the internal interactions when downloading results data from the object storage in the Results Service:

```mermaid
sequenceDiagram
  participant c as Client
  participant cp as Control Plane
  participant r as ResultsTable
  participant o as ObjectStorage

  c ->>+ cp: Submit tasks
  cp ->>+ r: Get result OpaqueID
  r -->>- cp: #

  cp ->>+ o: Initiate streamed download associated to OpaqueID
  o -->>- cp: #

  cp -->>- c: Streamed result data
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

  cp ->>+ t: Get tasks whose dependencies are completed by those results
  t -->>- cp: #

  cp ->>+ t: Set all tasks status to Pending
  t -->>- cp: #

  cp ->>+ q: Insert ready tasks into the Queue
  q -->>- cp: #

  cp ->>+ t: Set queued tasks status to Submitted
  t -->>- cp: #

  cp -->>- c: Updated result metadata
```


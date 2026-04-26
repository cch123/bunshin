# Architecture

This document maps the current Bunshin layers. All protocols and file formats shown here are Bunshin-native.

## Client And Driver

```mermaid
flowchart LR
    App["Application"]
    Pub["Publication API"]
    Sub["Subscription API"]
    Client["DriverClient"]
    IPC["Typed IPC rings"]
    Driver["MediaDriver"]
    Reports["Driver directory reports"]

    App --> Pub
    App --> Sub
    App --> Client
    Client --> IPC
    IPC --> Driver
    Driver --> Reports
    Driver --> Pub
    Driver --> Sub
```

Embedded mode uses in-process `Publication` and `Subscription` values. External mode keeps resource ownership behind the driver process and exposes operations through typed IPC commands with correlation IDs.

## Transport

```mermaid
flowchart LR
    Pub["Publication"]
    Terms["Term log"]
    Flow["Flow control"]
    QUIC["QUIC transport"]
    UDP["UDP transport"]
    Sub["Subscription"]
    Images["Images and lag reports"]
    Loss["Loss reports"]

    Pub --> Terms
    Pub --> Flow
    Flow --> QUIC
    Flow --> UDP
    QUIC --> Sub
    UDP --> Sub
    Sub --> Images
    Sub --> Loss
```

QUIC is the default reliable transport. UDP is explicit and supports Bunshin-native status frames, NAK repair, multicast, multi-destination sends, dynamic destinations, local spy observations, and name re-resolution.

## Archive

```mermaid
flowchart LR
    Sub["Subscription"]
    Archive["Archive"]
    Catalog["Recording catalog"]
    Segments["Segment files"]
    Replay["Replay"]
    Live["Live stream"]
    Merge["Replay merge"]
    Replication["Archive replication"]

    Sub --> Archive
    Archive --> Catalog
    Archive --> Segments
    Segments --> Replay
    Replay --> Merge
    Live --> Merge
    Archive --> Replication
```

The archive stores Bunshin messages and metadata in a Bunshin-native catalog and segment format. Replay merge and replication operate on those Bunshin recording descriptors.

## Cluster

```mermaid
flowchart LR
    Client["Cluster client"]
    Ingress["Ingress"]
    Consensus["Consensus module"]
    Log["Replicated log"]
    Service["Service container"]
    Snapshot["Snapshots"]
    Learner["Optional learner"]
    Backup["Backup or standby"]
    Control["Cluster control"]

    Client --> Ingress
    Ingress --> Consensus
    Consensus --> Log
    Log --> Service
    Service --> Snapshot
    Log --> Learner
    Log --> Backup
    Control --> Consensus
```

Cluster support is a Bunshin-native replicated-log service container with leader election, snapshot recovery, learners, backup/standby replication, and control operations. It does not implement Aeron Cluster wire compatibility.

## Observability

```mermaid
flowchart LR
    Metrics["Metrics"]
    Counters["Counter snapshots"]
    Errors["Error reports"]
    Loss["Loss reports"]
    CLI["bunshin-driver CLI"]
    Expvar["expvar and pprof example"]

    Metrics --> Counters
    Counters --> CLI
    Errors --> CLI
    Loss --> CLI
    Metrics --> Expvar
```

The driver writes JSON report files for counters, errors, and loss. `bunshin-driver` reads those reports and can request live snapshots or report flushes through IPC.

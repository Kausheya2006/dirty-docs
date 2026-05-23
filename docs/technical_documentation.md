# Technical Documentation

This document outlines the internal implementation, data structures, and network protocols driving DirtyDocs++.

## File Structure

```text
course-project-dirty-bits/
├── Makefile
├── README.md
├── docs/
│   └── technical_documentation.md
├── client/
│   └── client.c
├── common/
│   ├── config.h
│   ├── utils.c
│   └── utils.h
├── name_server/
│   ├── name_server.c
│   ├── ns_utils.c
│   └── ns_utils.h
└── storage_server/
    ├── storage_server.c
    ├── ss_utils.c
    └── ss_utils.h
```

## System Architecture Topology

The following diagram illustrates the split-plane architecture of DirtyDocs++, demonstrating the separation between metadata orchestration (Control Plane) and high-speed physical data transfer (Data Plane).

```text
=================================================================================
                          CONTROL PLANE (Metadata & Orchestration)
=================================================================================
                                 +-------------------------+
                                 |       NAME SERVER       |
                                 |-------------------------|
+----------------+   (1) Request | - N-ary Trie Map        | (2) Heartbeats & 
|                | ------------> | - Access Control Lists  | <------------------+
| USER CLIENT(S) | <------------ | - Node Health Monitor   | ----------------+  |
|  (Stateful UI) |   SS Socket   +-------------------------+    Commands     |  |
+----------------+                                                           |  |
      |  ^                                                                   |  |
      |  |                                                                   |  |
======|==|===================================================================|==|==
      |  |                        DATA PLANE (High-Speed IO)                 |  |
======|==|===================================================================|==|==
      |  |                                                                   |  |
      |  | (3) Direct Read/Write TCP Stream                                  |  |
      |  +--------------------------------------+                            |  |
      v                                         v                            |  |
+-------------------------+               +-------------------------+        |  |
|    STORAGE SERVER 1     | (4) Async IO  |    STORAGE SERVER 2     | <------+--+
|       (Primary)         | ------------> |       (Replica)         | 
|-------------------------|               |-------------------------|
| - Persistent Disk IO    |               | - Persistent Disk IO    |
| - Sentence-Level Locks  |               | - Sentence-Level Locks  |
| - Write-Ahead Backups   |               | - Write-Ahead Backups   |
+-------------------------+               +-------------------------+
```

## Component Breakdown

### 1. Common Directory (`common/`)

Contains shared definitions required across the distributed environment.

- `config.h`: Defines universal macros, error codes (`ERR_FILE_NOT_FOUND`, `ERR_SENTENCE_LOCKED`), buffer sizes, and standard port configurations. Ensures network protocol consistency.

- `utils.c / utils.h`: Implements shared helper functions, such as socket setup wrappers, robust string tokenization, and standardized logging functions used by all nodes.

### 2. Name Server (`name_server/`)

The orchestration layer. It runs a multi-threaded event loop accepting connections from both Storage Servers and Clients.

- `name_server.c`: The primary daemon. Binds to the designated metadata port, spawns worker threads for incoming requests, and runs a detached heartbeat-monitoring thread to detect Storage Server crashes.

- `ns_utils.c / ns_utils.h`: Implements the core metadata data structure.

    - Implementation detail: The file system is stored in RAM as an N-ary Tree/Trie. Each node represents a file or folder and holds a `Metadata` struct containing owner IDs, access control lists, and an array of `ss_ids` pointing to the primary and replica storage servers holding the physical data.

### 3. Storage Server (`storage_server/`)

The persistence layer.

- `storage_server.c`: Handles the startup handshake with the NM, transmitting its available files and capacities. Spawns a listener thread for NM control commands (e.g., `DELETE`, `CREATE`) and a separate listener for direct client data streams.

- `ss_utils.c / ss_utils.h`: Contains the complex file manipulation and locking logic.

    - Sentence-Level Locking: Implemented using an array of mutexes or a locked-index map tied to the file descriptor. When a client writes, the file content is dynamically parsed by delimiters (., ?, !) to isolate the target index before granting the lock.

    - Checkpointing: Implemented by writing serialized state diffs or `.bak` files during the write cycle, allowing O(1) reversion to previous states.

### 4. Client (`client/`)

The user interface layer.

- `client.c`: A stateful REPL (Read-Eval-Print Loop) utilizing GNU Readline.

    - Routing Logic: Parses user input. If the command alters metadata (e.g., `ADDACCESS`), it executes an RPC call to the Name Server. If the command requires data manipulation (e.g., `READ`, `WRITE`), it queries the NM for the SS socket address, disconnects, and establishes a new TCP session directly with the target Storage Server.

    - In-Place Editing: Leverages `rl_startup_hook` to inject server-provided strings into the input buffer for seamless modification.

## Fault Tolerance Implementation

1. Replication: When a `WRITE` transaction commits successfully on a primary SS, the SS issues an internal trigger to the NM. The NM dispatches a `REPLICATE` command to backup servers, duplicating the file data asynchronously without blocking the client.

2. Failure Detection: Storage Servers send a UDP or TCP heartbeat to the NM every `N` seconds. If the NM misses three consecutive heartbeats, it marks the SS as `DEAD` in the Trie map and promotes a replica to primary status.

3. Recovery: Upon reboot, an SS sends an `INIT_SYNC` request to the NM, comparing its local file hashes against the NM's master record and pulling missing updates before marking itself `ACTIVE`.
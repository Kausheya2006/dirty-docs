# DirtyDocs++

A highly concurrent, distributed collaborative file system written in C. 

<image src='assets/image.png' width=650>


DirtyDocs++ implements a decentralized architecture separating metadata orchestration (Name Server) from high-throughput data operations (Storage Servers). It supports real-time concurrent editing via sentence-level locking, fault tolerance through asynchronous replication, and an interactive, terminal-based client environment.

## Core Architecture

The system operates on a split-plane architecture:

* **Name Server (Control Plane):** Acts as the central directory. It maintains a highly efficient Trie-based metadata map of the file system, manages access control, monitors Storage Server health via heartbeats, and routes clients to the appropriate storage nodes.

* **Storage Servers (Data Plane):** Handle the physical persistence of files. They facilitate direct TCP streams to clients for reading/writing, enforce sentence-level distributed locks to prevent write conflicts, and maintain backup/checkpoint states.

* **Clients:** A stateful, GNU Readline-powered terminal interface that seamlessly navigates between communicating with the Name Server for metadata and opening high-speed direct sockets to Storage Servers for file transfers.

## Features

### File Operations & Concurrency

* **Sentence-Level Locking:** Multiple clients can concurrently edit the same document. The system isolates modifications at the sentence level (delimited by `.`, `!`, `?`), preventing race conditions while maximizing collaboration.

* **Real-time Streaming:** Supports simulated high-throughput streaming (`STREAM`) of file contents directly from the SS to the client.

* **Remote Execution:** Authorized clients can execute shell scripts stored on the file system (`EXEC`), evaluated securely by the Name Server with standard output piped back to the terminal.

* **Access Control & Security:** Granular Read/Write permissions (`ADDACCESS`, `REMACCESS`). File owners can dynamically alter access control lists.

### Advanced System Capabilities
* **Fault Tolerance & Replication:** Background, asynchronous replication of data across multiple Storage Servers. If a primary node fails, the Name Server automatically routes traffic to a replica.

* **Versioning & Checkpoints:** Users can snapshot file states at specific points in time (`CHECKPOINT`), view historical versions, and roll back changes instantly (`REVERT`, `UNDO`).

* **Hierarchical Storage:** Full support for nested directories, file migrations (`MOVE`), and recursive structural views (`VIEWFOLDER`).

* **O(1) / O(L) Lookups:** Metadata traversing and file lookups are optimized using Trie data structures within the Name Server memory space.

## Latest Update: In-Place Interactive Editing

The `WRITE` command has been completely overhauled. The legacy word-index loop (`ETIRW`) has been replaced with a fluid, in-place editing experience. When locking a sentence, the Storage Server pushes the existing string to the client. Utilizing `GNU Readline` buffer injection, the terminal pre-fills the prompt, allowing users to modify the text naturally using arrow keys before committing the transaction.

## Setup & Execution

### Prerequisites

* GCC compiler

* GNU Make

* GNU Readline library (`sudo apt-get install libreadline-dev` on Debian/Ubuntu)

### Building the Project

Navigate to the root directory and compile all components:

```bash
make clean
make all
```

### Running the System

The system must be booted in a specific order to establish the network topology.

1. Start the Name Server:

```bash
./nm
```

2. Start Storage Server(s):

```bash
./ss <ss_id> <nm_ip> <nm_port> <client_port>
# Example: ./ss 1 127.0.0.1 8080 9001
```
3. Start the Client:

```bash
./user
```

## Implementation Assumptions

The following constraints and behaviors were assumed during implementation as they were not explicitly defined in the project specification:

### 1. Indexing Strategy

* **1-Based Indexing**: Both Sentence numbers and Word indices are **1-based**.

### 2. File & Folder Constraints

* **Folder Deletion**: Folders cannot be deleted using the `DELETE` or `TRASH` commands. This is a safety measure to prevent recursive data loss.

* **Move Operations**: When moving a file using `MOVE`, if the destination is a folder, the file retains its original name within that folder. The destination folder must exist before moving.

### 3. System Limits & Configuration

* **Concurrency Limits**:

  * Max Storage Servers: **10**

  * Max Concurrent Clients: **100**

  * Thread Pool Workers: **10**

* **Network Ports**:

  * Name Server Command Port: `8080`

  * Name Server Heartbeat Port: `8081`

* **Replication**:

  * **Replication Factor**: **2** (Primary + 1 Replica). The system automatically selects 1 replica server in addition to the primary storage server.

* **Heartbeat Interval**: When we close the SS corresponding to a file, it takes 20 seconds for the NM to detect the failure and mark the SS as inactive. So during this interval, any client trying to access a file on that SS will get an error.

### 4. Persistence

* **Format**: Metadata is saved in a custom binary format (`NMTRIE02`) containing a magic header for versioning validation.

* **Scope**: Persistence saves the file structure (Trie), Access Control Lists (ACLs), and Trash state. It does *not* persist active client sessions.


## Documentation

Comprehensive documentation is available in the `docs/` directory, covering:

* **Technical Documentation:** Detailed breakdown of the codebase, data structures, and algorithms.

* **Version History:** A changelog of updates and feature additions.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.


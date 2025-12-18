# DirtyDocs++

**DirtyDocs++** is a distributed file system implementation for the "LangOS" project. This document outlines the specific implementation choices, assumptions, and unique features added beyond the core requirements.

## Unique Features (The "Unique Factor")

Beyond the standard requirements, the following features were implemented to enhance usability and safety:

### 1. Trash & Recovery System
Instead of immediate permanent deletion, this system implements a "Recycle Bin" mechanism to prevent accidental data loss.
* **`TRASH <filename>`**: Moves a file to a hidden trash state. The file becomes inaccessible for READ/WRITE operations but retains its metadata.
* **`RESTORE <filename>`**: Recovers a file from the trash to its original location.
* **`VIEWTRASH`**: Lists all files currently in the user's trash bin.
* **`EMPTYTRASH`**: Permanently deletes all files in the trash to free up space.
* *Note:* `DELETE` is retained for immediate permanent deletion as per requirements, but `TRASH` is recommended for safety.

### 2. Interactive Manual Pages
Integrated documentation system accessible directly from the CLI.
* **`man <command>`**: Displays detailed syntax, descriptions, and usage examples for any command (e.g., `man WRITE`, `man CHECKPOINT`).
* Eliminates the need to refer to external documentation while working.

### 3. Enhanced CLI Experience
* **GNU Readline Integration**: Supports command history (Up/Down arrows) and line editing.
* **Visual Feedback**: extensive use of ANSI color codes (Green for success, Red for errors, Cyan for UI frames) and box-drawing characters to create a structured, dashboard-like terminal interface.

---

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

---

## Architecture

The system deviates from a generic design by implementing specific high-performance data structures:

```
┌───────────────────┐       ┌───────────────────────────────────────┐
│     CLIENT        │       │          NAME SERVER (NM)             │
│                   │       │                                       │
│ ┌───────────────┐ │Request│ ┌─────────────┐ ┌───────────────────┐ │
│ │ GNU Readline  ├─┼───────┼►│ Task Queue  ├►│   Thread Pool     │ | 
│ └───────────────┘ │       │ └─────────────┘ │   (10 Workers)    │ │
│                   │       │                 └─────────┬─────────┘ │
│ ┌───────────────┐ │       │ ┌─────────────┐           │           │
│ │ Direct Stream │◄├─Data──┼─┤   Cache     │◄───────-──┘           │
│ └───────────────┘ │       │ └─────────────┘                       │
└─────────┬─────────┘       │ ┌─────────────┐ ┌───────────────────┐ │
          │                 │ │  Heartbeat  │ │  Trie (Metadata)  │ │
          │                 │ │   Monitor   │ └───────────────────┘ │
          │                 │ └──────┬──────┘                       │
          │                 └────────┼──────────────────────────────┘
          │                          │ Ping/Sync
          │                          ▼
          └────────────────►┌─────────────────────────────────────┐
                            │    STORAGE SERVER (SS)              │
                            │ ┌──────────┐ ┌──────────┐ ┌───────┐ │
                            │ │ SS_Data/ │ │Sent.Locks│ │Backup/│ │
                            │ └──────────┘ └──────────┘ └───────┘ │
                            └─────────────────────────────────────┘
```

1. **Metadata Management**: Uses a **Trie** (Prefix Tree) instead of a flat list or Hashmap for file lookups. This ensures O(L) lookup time (where L is filename length), enabling efficient hierarchical folder operations.

2. **Caching**: Implements a **Hash-based Cache** to store recently resolved File-to-SS mappings, reducing Trie traversal overhead for frequent accesses.

3. **NM**: Uses a fixed **Thread Pool** consuming from a synchronized circular **Task Queue**.
4. **SS**: Uses a **Thread-per-Connection** model to handle multiple simultaneous client streams (READ/WRITE) and NM commands.

5. **Locking**: The Storage Server maintains a linked list of active **Sentence Locks** protected by mutexes, allowing multiple users to edit different sentences of the same file simultaneously.

---

Created by Team: `DirtyBits`

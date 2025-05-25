# High-Level Architecture of the Rust BitTorrent Client

This BitTorrent client is built with a modular architecture designed for clarity and efficient concurrent operation. The system comprises specialized components, each with a distinct responsibility, collaborating to download files. Communication is primarily asynchronous.

## Core Principles

* **Modularity**: The system is broken down into distinct components, each with a clear responsibility. This makes the system easier to understand, develop, and maintain.
* **Concurrency**: Many operations, like talking to multiple peers or managing several torrents, happen at the same time. The architecture relies on asynchronous operations (Rust's `async/await` using `Tokio` crate) and message passing between components to handle this efficiently without blocking.

## Main Architectural Components

Here's a breakdown of the key components and their roles:

1. **`TorrentManager`**
    * **Role**: The top-level component overseeing and managing all active torrents within the client.

2. **`Torrent`**
    * **Role**: Represents and manages a single torrent. It's responsible for parsing the torrent's metadata and handling interactions with trackers to discover peers.

3. **Core Download Engine (Per-Torrent Components):**
    Each active `Torrent` utilizes a dedicated set of core components to drive its download process:

    * **`Orchestrator`**
        * **Role**: The central coordinator for all download activities of a *single torrent*. It facilitates communication between the torrent's other core components and its active peer connections.

    * **`Synchronizer`**
        * **Role**: Determines the strategy for piece selection and tracks the overall download progress and piece availability for its torrent.

    * **`Monitor`**
        * **Role**: Manages the pool of peer connections for its torrent, establishing new connections and monitoring existing ones.

    * **`DiskWriter`**
        * **Role**: Responsible for writing successfully downloaded file pieces to the disk for its torrent.

4. **Peer Interaction Layer (Per-Peer Connection Components):**
    For each connection established with a remote peer, a specific set of components manages the direct interaction:

    * **`PeerSession`**
        * **Role**: Manages the complete lifecycle of communication with a single remote peer, including message handling and connection state.

    * **`Coordinator` (within `PeerSession`)**
        * **Role**: Manages the tactical aspects of downloading pieces from the specific peer associated with its `PeerSession`. It queues requests and tracks their progress for that peer.

    * **`io` (within `PeerSession`)**
        * **Role**: Handles the low-level network input/output for its `PeerSession`, sending messages to and receiving messages from the connected peer.

## Conceptual Diagram

![image](/docs/architecture_diagram.png)

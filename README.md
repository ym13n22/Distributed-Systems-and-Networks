This project implements a simplified Distributed File System in Java, consisting of:

A Controller that manages Dstore nodes, handles client requests, maintains file metadata (index), and coordinates rebalancing.

Multiple Dstores that store replicated file data and respond to file operations.

A Client (provided) that sends store, load, list, and remove requests to the Controller.

All communication occurs over TCP sockets, and the system handles concurrency, failure, and rebalance logic.

🔧 Controller - Responsibilities & Functions
Dstore Join Management

Accepts JOIN <port> messages from Dstores.

Tracks connected Dstores.

Waits until at least R Dstores have joined before accepting client operations.

File Index Management

Maintains metadata for all files (including size, status, and which Dstores hold them).

Handles file states: store in progress, store complete, remove in progress.

Client Request Handling

STORE filename filesize → selects R Dstores and replies with STORE_TO.

LOAD filename → replies with LOAD_FROM (a Dstore and file size).

REMOVE filename → notifies all Dstores storing the file.

LIST → replies with list of all stored (complete) filenames.

Receiving Messages from Dstores

Handles STORE_ACK, REMOVE_ACK, REBALANCE_COMPLETE, and errors.

Monitors file replication and removal progress.

Failure Handling

Detects unresponsive Dstores via timeouts and removes them.

Abandons operations that don’t complete in time.

Does not attempt to reconnect to failed Dstores.

Rebalancing

Initiated via command-line or automatically.

Collects file lists from all Dstores.

Determines send/delete plan to ensure:

Each file is replicated to R Dstores.

File distribution is balanced across Dstores.

Sends REBALANCE commands with instructions.

Waits for REBALANCE_COMPLETE.

💾 Dstore - Responsibilities & Functions
Joining the Controller

Sends JOIN <port> on startup.

Maintains a persistent connection with the Controller.

Client Interactions

Handles STORE filename filesize → receives and stores the file.

Handles LOAD_DATA filename → sends file content to client.

Controller Interactions

REMOVE filename → deletes the file and sends REMOVE_ACK.

LIST → replies with list of locally stored filenames.

REBALANCE → receives:

Files to send to other Dstores via REBALANCE_STORE.

Files to delete locally.

Sends REBALANCE_COMPLETE upon finishing.

Local File Management

Uses a clean storage folder on startup.

Stores files under unique directory per instance (port-based).

Failure Handling

Does not reconnect to Controller if disconnected.

Will be removed from the system by Controller if unresponsive.

👤 Client - Overview (already implemented)
Controller Communication

Sends STORE, LOAD, REMOVE, and LIST commands.

Interprets Controller’s responses to proceed.

Dstore Communication

Uploads file content to selected Dstores.

Downloads file from one Dstore.

Retries with RELOAD on read failures.

Compilation
This project is written in standard Java 21 (openjdk-21-jdk) with no dependencies or packages.

To compile all .java files:

javac *.java
Running the System
You must run each component from the command line in separate terminals (all on the same machine).

1. Start the Controller

java Controller <cport> <R> <timeout> <rebalance_period>
cport: Port Controller listens on (e.g., 12345)

R: Replication factor (e.g., 2 or 3)

timeout: Timeout in milliseconds (e.g., 1000)

rebalance_period: Seconds between automatic rebalances (e.g., 30)

Example:

java Controller 12345 2 1000 30
2. Start the Dstores
You must start at least R Dstores. Use different ports and folders for each.


java Dstore <port> <cport> <timeout> <file_folder>
port: Port the Dstore listens on (e.g., 2001)

cport: Controller’s port (must match the Controller's cport)

timeout: Timeout in milliseconds

file_folder: Path to store files (must exist and be different per Dstore)

Example:


java Dstore 2001 12345 1000 dstore1_folder
3. Start the Client
The client is provided as a JAR file (client.jar) and communicates with the Controller.


java -jar client.jar <cport> <timeout>
Example:

java -jar client.jar 12345 1000
File Structure Requirements
No Java packages used.

All .java files must be in the same directory.

Dstore data folders must already exist before starting Dstores.

The system assumes that files are less than 100KB and not empty.

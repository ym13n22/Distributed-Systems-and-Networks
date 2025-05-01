This project implements a simplified Distributed File System in Java, consisting of:

A Controller that manages Dstore nodes, handles client requests, maintains file metadata (index), and coordinates rebalancing.

Multiple Dstores that store replicated file data and respond to file operations.

A Client (provided) that sends store, load, list, and remove requests to the Controller.

All communication occurs over TCP sockets, and the system handles concurrency, failure, and rebalance logic.

🔧 Controller – Key Responsibilities

Dstore Management: Accepts JOIN requests and tracks connected Dstores. Waits for at least R Dstores before allowing client operations.

File Metadata: Maintains file info (size, status, Dstore locations). Tracks states like store in progress or store complete.

Client Requests:

STORE: Selects R Dstores and replies with STORE_TO.

LOAD: Replies with LOAD_FROM.

REMOVE: Sends delete commands to Dstores.

LIST: Returns list of completed files.

Dstore Communication: Handles STORE_ACK, REMOVE_ACK, REBALANCE_COMPLETE, and error signals.

Failure Handling: Removes unresponsive Dstores and aborts incomplete operations.

Rebalancing: Ensures each file is stored on R Dstores and balances load. Sends REBALANCE instructions and waits for completion.

💾 Dstore – Key Responsibilities

Controller Join: Sends JOIN <port> on startup and maintains the connection.

Client Operations:

STORE: Receives and stores files.

LOAD_DATA: Sends file content to client.

Controller Commands:

REMOVE: Deletes file, replies with REMOVE_ACK.

LIST: Returns list of stored files.

REBALANCE: Sends specified files to other Dstores, deletes files, then replies REBALANCE_COMPLETE.

Storage: Uses a clean, port-based directory.

Failure: Does not reconnect if disconnected; will be removed by the Controller.

👤 Client – Summary

Communicates with Controller using STORE, LOAD, REMOVE, and LIST.

Uploads and downloads files from Dstores.

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

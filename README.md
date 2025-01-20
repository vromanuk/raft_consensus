# Raft Consensus

This project is the result of the [Rafting Trip](https://www.dabeaz.com/raft.html) course. It implements a key-value
store using the Raft consensus algorithm.

## Overview

The Raft consensus algorithm is used to manage a replicated log. This project demonstrates a simple implementation of
Raft to achieve consensus across a cluster of servers.

## Running the Raft Consensus Application

### 1. Start the Cluster

To start the cluster, run the following command:

```bash
python3 raft_kv_server.py <N>
```

> Note: The cluster configuration relies on RAFT_SERVERS defined in raft_configuration.py. Ensure that the number of
> servers you start matches the number specified in RAFT_SERVERS, or adjust the cluster size to 3 servers for testing
> purposes. It's critical for reaching consensus that the majority of servers are running.

### 2. Start the Client:

To interact with the cluster, start the client using:

```bash
python3 raft_kv_client.py <leader_id>
```

> <leader_id>: The ID of the server designated as the leader. You can identify the leader by looking for Received append
> entries response from logs in the server console output. Look for `Received append entries response from` logs in the
> console that indicates the leader.

### 3. Send Commands to the Client:

Once the client is connected, you can send commands to interact with the key-value store:

```
KV 2 > set vlad vlad
Received > ok
KV 2 > get vlad
Received > vlad
KV 2 > set 42 42
Received > ok
```

> Checkout `raft_kv_app.py` for a list of available commands.
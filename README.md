# Networks: peer-to-peer key/value store

Coursework for networks module. A node for a distributed key/value network in Java, communicating over UDP.

## What it does

Each node gets an ID by hashing its name with SHA-256. It finds other nodes by "distance" between hashes, and stores and retrieves key/value pairs across the network.

- **Protocol:** encodes and decodes the network's message format, including name, nearest-node, key-exists, read, write, compare-and-swap and relay messages
- **Reliability over UDP:** retransmits after 5 seconds, gives up after 3 retries, detects duplicate packets and matches responses by transaction ID
- **Address table:** keeps at most 3 known nodes per distance
- **Robustness:** ignores malformed messages and protects against resource exhaustion

## Files

| File | Purpose |
|---|---|
| `Node.java` | The node: protocol, storage, retries and relaying |
| `HashID.java` | SHA-256 hashing and distance calculation |
| `LocalTest.java` | Starts several nodes on one machine and tests them together |
| `AzureLabTest.java` | Connects to the university's test network |

## Running it

```bash
cd CRN_Coursework
javac *.java
java LocalTest 5
```

## Known limitations

- Data rebalancing when nodes join or leave isn't fully implemented

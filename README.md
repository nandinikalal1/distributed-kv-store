# Distributed Key-Value Store (DynamoDB-style) 
Java + Spring Boot

A DynamoDB-inspired distributed key-value store built in Java. The system runs as a small multi-node cluster with a gateway (coordinator) and three storage nodes. It demonstrates core distributed-systems concepts used in systems like DynamoDB/Cassandra: partitioning via consistent hashing, replication, quorum reads/writes, failure detection, read repair, and hinted handoff.
   
            Client
              |
              v
       +-------------------+
       |   Gateway         |
       |  (Coordinator)    |
       +-------------------+
          |       |       |
          v       v       v
      +------+ +------+ +------+
      |Node1 | |Node2 | |Node3 |
      |8081  | |8082  | |8083  |
      +------+ +------+ +------+

## What this system does
- Stores key → value data across multiple machines
- Uses **consistent hashing** to partition keys
- Replicates each key to **N = 3** nodes
- Supports **quorum writes (W)** and **quorum reads (R)**
- Uses **versioning** to resolve conflicts (latest version wins)
- Detects node failures using health checks
- Uses **read repair** and **hinted handoff** to recover from failures

## Architecture
- **Gateway (port 8090)**: coordinates all reads/writes, handles quorum logic and failure recovery
- **Storage Nodes (ports 8081, 8082, 8083)**: store data in-memory and expose simple REST APIs

Clients communicate only with the gateway.

## How to run

Open 4 terminals in the project root.

```bash
# Node 1
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8081

# Node 2
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8082

# Node 3
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8083

# Gateway
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8090

Example usage
1. Quorum write (W=2)
curl -X PUT "http://localhost:8090/gkv/put?key=user1&value=hello&w=2"

2. Quorum read (R=2)
curl "http://localhost:8090/gkv/get?key=user1&r=2"

Failure handling

Writes succeed as long as quorum is met, even if a node is down

Missed writes are stored as hints

When a node recovers, the gateway automatically replays hints (hinted handoff)

Concepts covered
1. Consistent hashing
2. Replication
3. Quorum (R/W/N)
4. Eventual consistency
5. Read repair
6. Hinted handoff
7. Failure detection

Summary

This project shows how large-scale systems like DynamoDB remain highly available during failures while ensuring eventual consistency through quorum and recovery mechanisms.
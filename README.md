# Distributed Cloud Object Store

A distributed cloud object storage system capable of storing and retrieving arbitrary objects across multiple storage nodes with chunking, replication, failure handling, and data integrity verification.

## Architecture
- **Controller Node**: 1 Coordinator managing metadata, chunk placement, and worker health.
- **Worker Nodes**: 4 Storage nodes with persistent storage.
- **Replication Factor**: 2 copies per chunk.
- **Chunk Size**: 1 MB.

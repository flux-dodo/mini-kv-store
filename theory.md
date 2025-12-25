# Storage Engine Index Structures

## Hash Indexes

A **hash index** maintains an in-memory hash table that maps each key to the location (offset) of its value in an append-only data file.

- All writes are appended sequentially to a **segment file** on disk.
- The in-memory hash table stores:  
  `key → file offset`
- Deletes are represented using **tombstones** instead of removing data in place.
- Periodic **compaction** rewrites segment files to:
  - Remove obsolete values
  - Discard tombstones
  - Keep only the latest value per key
- Segment files are stored on disk; only the hash index is kept in memory.

**Advantages**
- Fast writes due to sequential disk I/O
- Simple design

**Limitations**
- The entire hash index must fit in memory
- No efficient range queries
- Poor scalability for large keyspaces

Hash indexes are mainly useful as a conceptual baseline rather than a general-purpose database index.

---

## LSM Trees (Log-Structured Merge Trees) and SSTables

An **LSM tree** is a write-optimized storage structure built on top of **Sorted String Tables (SSTables)**.

### SSTables
- An SSTable is an immutable file storing key–value pairs **sorted by key**.
- Sorted order enables:
  - Binary search for lookups
  - A **sparse in-memory index** instead of storing offsets for all keys
- Multiple SSTables can be merged efficiently using **merge sort**.

### Write Path
1. Writes are applied to an in-memory sorted structure called the **MemTable** (typically a balanced BST or skip list).
2. Every write is also appended to a **Write-Ahead Log (WAL)** for crash recovery.
3. When the MemTable exceeds a size threshold, it is flushed to disk as a new SSTable.

### Read Path
Reads are served by checking:
1. MemTable
2. Newest SSTables
3. Older SSTables

The first matching key encountered is returned.

### Compaction
- Over time, multiple SSTables accumulate, increasing read amplification.
- **Compaction** merges SSTables into fewer, larger tables:
  - Keeps only the most recent value per key
  - Removes duplicate entries
  - Deletes keys marked with tombstones
- Compaction is implemented as a merge-sort over sorted files.

### Trade-offs
- High write throughput due to sequential writes
- Increased read amplification
- Additional write amplification caused by compaction

**Used by:** Apache Cassandra, time-series databases, RocksDB

---

## B-Trees

A **B-tree** stores keys in a balanced tree structure where each node corresponds to a fixed-size **disk page**.

- The tree is stored in linear disk storage using pages.
- Reads traverse the tree from root to leaf, resulting in predictable `O(log n)` page accesses.
- Updates modify pages in place, leading to random disk I/O.

### Write-Ahead Logging (WAL)
- Because B-tree pages are updated in place, a **Write-Ahead Log** is required.
- Every modification must be written to the WAL **before** updating the tree pages.
- During crash recovery, the WAL is replayed to restore consistency.

### Trade-offs
- Faster reads due to structured indexing
- Slower writes due to random disk access
- Lower read amplification compared to LSM trees

**Used by:** relational databases, MongoDB

---

## LSM Trees vs B-Trees

- **LSM trees** optimize for write-heavy workloads by converting random writes into sequential disk writes, at the cost of higher read and write amplification.
- **B-trees** optimize for read-heavy workloads by maintaining an ordered on-disk structure, at the cost of slower writes.

The choice between LSM trees and B-trees depends primarily on whether the workload prioritizes **writes** or **reads**.

# Mini Key–Value Store (Java)

## Tech Stack

- **Language:** Java
- **Java version:** Java 17+ (works with Java 11 if `record` is replaced)
- **Build tool:** none (plain `javac`)
- **Dependencies:** none
- **Storage format:** custom binary (WAL + SST)

---

## LSM-based

A minimal, educational implementation of an **LSM-tree–based key–value store** written in plain Java.

### Features

- Append-only **Write Ahead Log (WAL)** for durability
- In-memory **MemTable** (sorted map)
- Immutable **SSTables** written to disk
- **Sparse index** inside SSTables
- **Tombstones** for deletes
- **Full compaction** when SST count crosses threshold
- Crash recovery by replaying WAL
- Simple HTTP interface (`PUT`, `GET`, `DELETE`)
- Verbose logs to visualize internal behavior (flush, compaction, recovery)

### How to Compile and Run

From the project root:

```bash
rm -rf data/lsm*
mkdir -p data/lsm/sst
javac -d out $(find src -name "*.java")
java -cp out -Dengine=lsm store.Main
```

Expected Output

```bash
KV server started on port 8080
Use:
  PUT    /store?key=k   (body = value bytes)
  GET    /store?key=k
  DELETE /store?key=k
```

#### Basic Usage Example
Open another terminal and run:

```bash
curl -X PUT "http://localhost:8080/store?key=a" --data-binary "v1"
curl -X PUT "http://localhost:8080/store?key=a" --data-binary "v2"
curl "http://localhost:8080/store?key=a"
```

Output: Newest value
```bash
v2
```

Delete the key:
```bash
curl -X DELETE "http://localhost:8080/store?key=a"
curl "http://localhost:8080/store?key=a"
```

Output: Empty response

#### Restart & Recovery Demo

```bash
java -cp out -Dengine=lsm store.Main
```

Example logs
```bash
[WAL_RECOVERY] Applied=8 bytesConsumed=232
[RECOVERY] Replayed WAL entries=8 memBytes=168 memKeys=8
[COMPACT_SKIP] sstCount=1 trigger=4
KV server started on port 8080
```

This shows:
- WAL replay

MemTable reconstruction

Optional compaction on startup

#### Creating SSTables (MemTable Flush)
Trigger a flush by inserting enough keys:

```bash
for i in {1..30}; do
  curl -s -X PUT "http://localhost:8080/store?key=k$i" --data-binary "value-$i" >/dev/null
done
```

Example logs:
```bash
[FLUSH_TRIGGER] memBytes=258 threshold=256
[FLUSH_START] sstTmp=sst-000001.dat.tmp keys=26 tombstones=1 memBytes=258
[FLUSH_WRITE] sstOut=sst-000001.dat bytes=582
[MANIFEST] added=sst-000001.dat totalSst=1
[FLUSH_DONE] clearedMemTable resetWAL
```

This shows:
- MemTable crossing flush threshold
- SSTable creation
- WAL reset

#### Triggering Compaction
Run multiple overwrites to generate many SSTables:

```bash
for i in {1..50}; do
  curl -s -X PUT "http://localhost:8080/store?key=k$i" --data-binary "value-firstrun-$i" >/dev/null
done

for i in {1..50}; do
  curl -s -X PUT "http://localhost:8080/store?key=k$i" --data-binary "value-secondrun-$i" >/dev/null
done
```

Example compaction logs:

```bash
[COMPACT_TRIGGER] sstCount=4 trigger=4
[COMPACT_START] inputs=4 bytes=1763 outTmp=sst-000005.dat.tmp
[COMPACT_READ] file=sst-000004.dat entries=13 shadowed=0 mergedKeys=13
[COMPACT_READ] file=sst-000003.dat entries=13 shadowed=0 mergedKeys=26
[COMPACT_READ] file=sst-000002.dat entries=13 shadowed=0 mergedKeys=39
[COMPACT_READ] file=sst-000001.dat entries=14 shadowed=3 mergedKeys=50
[COMPACT_MERGE] filesRead=4 recordsRead=53 keptValues=50 keptTombstones=0 shadowed=3 tombstonesDropped=0
[COMPACT_WRITE] out=sst-000005.dat bytes=1593 inputBytes=1763 writeAmplification=2.11
[MANIFEST] replaced inputs=4 with out=sst-000005.dat totalSst=1
[COMPACT_DONE] deletedOld=4
```

This shows:
- Merge of multiple SSTables
- Newest-wins semantics
- Removal of shadowed entries
- Write amplification
- Manifest update
- Deletion of old SST files


## B-Tree Based

A minimal disk-backed B-Tree key–value store implemented from scratch to contrast with the LSM approach.

Unlike the LSM engine, the B-Tree:
- performs in-place page updates
- optimizes read latency
- relies on a page-image Write-Ahead Log (WAL) for crash safety

### Features
- Fixed-size pages stored in a single data file (btree.data)
- Leaf & internal pages with binary search
- Page splits (leaf + internal)
- Root split handling
- Page-image WAL (redo-only) for crash recovery
- Atomic updates using: WAL → fsync → page write → fsync → meta persist
- On-startup WAL replay into page file
- Metadata persisted separately (meta.txt)
- Same KV interface as LSM engine

### How to Compile and Run

From the project root:

```bash
rm -rf data/btree*
mkdir -p data/btree
javac -d out $(find src -name "*.java")
java -cp out -Dengine=btree store.Main
```

Expected Output

```bash
Initialized new B-Tree with root page 0
KV server started on port 8080
Use:
  PUT    /store?key=k   (body = value bytes)
  GET    /store?key=k
  DELETE /store?key=k
```

#### Basic Usage Example
Open another terminal and run:

```bash
curl -X PUT "http://localhost:8080/store?key=a" --data-binary "v1"
curl -X PUT "http://localhost:8080/store?key=a" --data-binary "v2"
curl "http://localhost:8080/store?key=a"
```

Output: Newest value
```bash
v2
```

#### Leaf Page Split Demo
With MAX_KEYS_PER_PAGE = 3, inserting 4 keys forces a leaf split.

```bash
curl -X PUT "http://localhost:8080/store?key=k1" --data-binary "v1"
curl -X PUT "http://localhost:8080/store?key=k2" --data-binary "v2"
curl -X PUT "http://localhost:8080/store?key=k3" --data-binary "v3"
curl -X PUT "http://localhost:8080/store?key=k4" --data-binary "v4"
```

Example logs
```bash
Leaf split: promote key='k2' to parent
Root split: new root pageId=2
```

This shows:
- Leaf page overflow
- Split into left + right leaf
- Key promotion
- New root creation

#### Internal Page Split Demo

```bash
for i in {5..20}; do
  curl -s -X PUT "http://localhost:8080/store?key=k$i" --data-binary "v$i" >/dev/null
done
```

Example logs
```bash
Internal split: promote key='k3' to parent
```

This demonstrates:
- Propagation of splits upward
- Structural rebalancing
- Tree height growth

#### Restart & Recovery Demo
To test crash recovery run with wal persist flag as true.

```bash
java -cp out -Dengine=btree -Dwal.persist=true store.Main
```

In another terminal, send request to send data
```bash
for i in {5..20}; do
  curl -s -X PUT "http://localhost:8080/store?key=k$i" --data-binary "v$i" >/dev/null
done
```

Stop and restart server

Example logs
```bash
ja -cp out -Dengine=btree store.Main
WAL replay applied 79 pages
KV server started on port 8080
```

This shows:
- WAL replay
- Pages were restored correctly
- No torn or partial writes
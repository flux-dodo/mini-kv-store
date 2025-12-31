// MiniLsmKV.java
package store.lsm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import store.KV;
import store.lsm.MemTable.ValueRecord;

import static java.nio.file.StandardCopyOption.*;

/**
 * A minimal LSM key-value store implementation.
 */
public final class MiniLsmKV implements KV{
  private final Wal wal;
  private final MemTable mem;
  private final Manifest manifest;
  private final Path dataDir;
  private final long memFlushBytes;
  private final int compactTrigger;

  public static final int SPARSE_EVERY = 4;
  private boolean compactionRunning = false;

  public MiniLsmKV(Path dataDir, long memFlushBytes, int compactTrigger) throws IOException {
    this.dataDir = dataDir;
    this.memFlushBytes = memFlushBytes;
    this.compactTrigger = compactTrigger;

    Files.createDirectories(dataDir.resolve("sst"));
    this.manifest = Manifest.loadOrCreate(dataDir.resolve("manifest.txt"));
    this.wal = new Wal(dataDir.resolve("wal.log"));
    this.mem = new MemTable();

    log("INIT", "dataDir=%s memFlushBytes=%d compactTrigger=%d sparseEvery=%d",
        dataDir.toAbsolutePath(), memFlushBytes, compactTrigger, SPARSE_EVERY);

    // Recovery
    int replayed = wal.replayInto(mem); // <-- change replayInto to return count (recommended)
    log("RECOVERY", "Replayed WAL entries=%d memBytes=%d memKeys=%d",
        replayed, mem.approxBytes(), mem.size()); // <-- add size() to MemTable (recommended)

    // Optional: compact on startup if too many tables
    maybeCompact();
  }

  /**
   * Puts a key-value pair into the store.
   */
  @Override
  public synchronized void put(String key, byte[] value) throws IOException {
    wal.appendPut(key, value);
    mem.put(key, value);

    long bytes = mem.approxBytes();
    log("PUT", "key=%s valueBytes=%d memBytes=%d/%d", key, value.length, bytes, memFlushBytes);

    if (bytes >= memFlushBytes) {
      log("FLUSH_TRIGGER", "memBytes=%d threshold=%d", bytes, memFlushBytes);
      flushMemTable();
      maybeCompact();
    }
  }

  /**
   * Deletes a key from the store.
   */
  @Override
  public synchronized void delete(String key) throws IOException {
    wal.appendDelete(key);
    mem.delete(key);

    long bytes = mem.approxBytes();
    log("DEL", "key=%s memBytes=%d/%d", key, bytes, memFlushBytes);

    if (bytes >= memFlushBytes) {
      log("FLUSH_TRIGGER", "memBytes=%d threshold=%d", bytes, memFlushBytes);
      flushMemTable();
      maybeCompact();
    }
  }

  /**
   * Gets the value for a key from the store.
   */
  @Override
  public synchronized byte[] get(String key) throws IOException {
    var inMem = mem.get(key);
    if (inMem != null) {
      if (inMem.tombstone()) {
        log("GET", "key=%s hit=MEM result=TOMBSTONE", key);
        return null;
      }
      log("GET", "key=%s hit=MEM valueBytes=%d", key, inMem.value().length);
      return inMem.value();
    }

    var sstFiles = manifest.sstablesNewestFirst(dataDir.resolve("sst"));
    int checked = 0;
    for (Path sst : sstFiles) {
      checked++;
      ValueRecord v = SstHelper.get(sst, key);
      if (v == null) continue;

      if (v.tombstone()) {
        log("GET", "key=%s hit=SST file=%s checked=%d/%d result=TOMBSTONE",
            key, sst.getFileName(), checked, sstFiles.size());
        return null;
      }

      log("GET", "key=%s hit=SST file=%s checked=%d/%d valueBytes=%d",
          key, sst.getFileName(), checked, sstFiles.size(), v.value().length);
      return v.value();
    }

    log("GET", "key=%s hit=MISS checkedSst=%d", key, sstFiles.size());
    return null;
  }

  /**
   * Flushes the in-memory table to a new SSTable on disk.
   * @throws IOException
   */
  private void flushMemTable() throws IOException {
    if (mem.isEmpty()) {
      log("FLUSH_SKIP", "MemTable is empty");
      return;
    }

    int id = manifest.nextId();
    Path out = dataDir.resolve("sst").resolve(String.format("sst-%06d.dat", id));
    Path tmp = out.resolveSibling(out.getFileName() + ".tmp");

    var snap = mem.snapshot();
    long beforeBytes = mem.approxBytes();
    int beforeKeys = snap.size();
    int tombstones = (int) snap.values().stream().filter(ValueRecord::tombstone).count();

    log("FLUSH_START", "sstTmp=%s keys=%d tombstones=%d memBytes=%d",
        tmp.getFileName(), beforeKeys, tombstones, beforeBytes);

    SstHelper.write(tmp, snap, SPARSE_EVERY);
    Files.move(tmp, out, ATOMIC_MOVE);

    long outSize = Files.size(out);
    log("FLUSH_WRITE", "sstOut=%s bytes=%d", out.getFileName(), outSize);

    manifest.addSstable(out.getFileName().toString());
    manifest.persistAtomically(dataDir.resolve("manifest.txt"));
    log("MANIFEST", "added=%s totalSst=%d", out.getFileName(), manifest.sstableCount());

    mem.clear();
    wal.reset();
    log("FLUSH_DONE", "clearedMemTable resetWAL");
  }

  /**
   * Maybe triggers a compaction if the number of SSTables exceeds the threshold.
   * @throws IOException
   */
  private synchronized void maybeCompact() throws IOException {
      int count = manifest.sstableCount();
      if (count < compactTrigger) {
          log("COMPACT_SKIP", "sstCount=%d trigger=%d", count, compactTrigger);
          return;
      }
      if (compactionRunning) {
          log("COMPACT_SKIP", "already running");
          return;
      }

      compactionRunning = true;
      try {
          log("COMPACT_TRIGGER", "sstCount=%d trigger=%d", count, compactTrigger);
          Compactor.compactAll(dataDir, manifest);
      } finally {
          compactionRunning = false;
      }
  }

  /**
   * Logs a formatted message with a tag.
   * @param tag The tag to categorize the log message.
   * @param fmt The format string.
   * @param args Arguments referenced by the format specifiers in the format string.
   */
  private static void log(String tag, String fmt, Object... args) {
      System.out.print("[" + tag + "] ");
      System.out.printf(fmt + "%n", args);
  }
}

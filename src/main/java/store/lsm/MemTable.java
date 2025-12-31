package store.lsm;

import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * In-memory MemTable for LSM store.
 * Supports basic put/get/delete/contains etc MAP operations.
 */
public class MemTable {
    private final TreeMap<String, ValueRecord> mem;
    private static final int SIZE = 4;

    public static record ValueRecord(byte[] value, boolean tombstone) {}

    public MemTable() {
        this.mem = new TreeMap<>();
    }

    public synchronized void put(String key, byte[] value) {
        mem.put(key, new ValueRecord(value, false));
    }

    public synchronized void delete(String key) {
        mem.put(key, new ValueRecord(null, true));
    }

    public synchronized ValueRecord get(String key) {
        return mem.get(key);
    }

    public synchronized boolean contains(String key) {
        return mem.containsKey(key);
    }

    public synchronized boolean isFull() {
        return mem.size() >= SIZE;
    }

    /** Snapshot for flush/compaction without holding MemTable lock for long. */
    public synchronized NavigableMap<String, ValueRecord> snapshot() {
        return new TreeMap<>(mem);
    }

    public synchronized long approxBytes() {
        long total = 0;
        for (var entry : mem.entrySet()) {
            total += entry.getKey().getBytes(StandardCharsets.UTF_8).length;
            var vrec = entry.getValue();
            if (!vrec.tombstone() && vrec.value() != null) {
                total += vrec.value().length;
            }
        }
        return total;
    }

    public synchronized boolean isEmpty() {
        return mem.isEmpty();
    }

    public synchronized void clear() {
        mem.clear();
    }

    public synchronized int size() { return mem.size(); }
}

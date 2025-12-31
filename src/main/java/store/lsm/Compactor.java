// Compactor.java
package store.lsm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Compactor for LSM storage engine.
 */
public final class Compactor {

    private Compactor() {}

    public static void compactAll(Path dataDir, Manifest manifest) throws IOException {
        Path sstDir = dataDir.resolve("sst");

        List<String> inputNames = manifest.sstablesOldestToNewest();
        if (inputNames.size() < 2) {
            log("COMPACT_SKIP", "not enough SSTables: %d", inputNames.size());
            return;
        }

        List<Path> inputPaths = new ArrayList<>();
        long inputBytes = 0;
        for (String name : inputNames) {
            Path p = sstDir.resolve(name);
            inputPaths.add(p);
            if (Files.exists(p)) inputBytes += Files.size(p);
        }

        int id = manifest.nextId();
        String outName = String.format("sst-%06d.dat", id);
        Path out = sstDir.resolve(outName);
        Path tmp = out.resolveSibling(out.getFileName() + ".tmp");

        log("COMPACT_START", "inputs=%d bytes=%d outTmp=%s",
            inputNames.size(), inputBytes, tmp.getFileName());

        // Merge result (newest wins)
        MemTable merged = new MemTable();

        int filesRead = 0;
        long recordsRead = 0;
        long keysKept = 0;
        long tombstonesKept = 0;
        long shadowed = 0;

        var newestFirst = manifest.sstablesNewestFirst(sstDir);
        for (Path sst : newestFirst) {
            filesRead++;
            NavigableMap<String, MemTable.ValueRecord> entries = SstHelper.readAll(sst);
            recordsRead += entries.size();

            int shadowedInFile = 0;

            for (var entry : entries.entrySet()) {
                String key = entry.getKey();
                MemTable.ValueRecord vr = entry.getValue();

                if (merged.contains(key)) {
                    shadowed++;
                    shadowedInFile++;
                    continue; // already have newer version
                }

                if (vr.tombstone()) {
                    merged.delete(key);
                    tombstonesKept++;
                } else {
                    merged.put(key, vr.value());
                    keysKept++;
                }
            }

            log("COMPACT_READ", "file=%s entries=%d shadowed=%d mergedKeys=%d",
                sst.getFileName(), entries.size(), shadowedInFile, merged.size());
        }

        // Drop tombstones only because we compact ALL sstables into one.
        NavigableMap<String, MemTable.ValueRecord> snap = merged.snapshot();
        int tombstonesBeforeDrop = (int) snap.values().stream().filter(MemTable.ValueRecord::tombstone).count();
        snap.entrySet().removeIf(e -> e.getValue().tombstone());
        int tombstonesDropped = tombstonesBeforeDrop;

        log("COMPACT_MERGE",
            "filesRead=%d recordsRead=%d keptValues=%d keptTombstones=%d shadowed=%d tombstonesDropped=%d",
            filesRead, recordsRead, keysKept, tombstonesKept, shadowed, tombstonesDropped);

        SstHelper.write(tmp, snap, MiniLsmKV.SPARSE_EVERY);
        Files.move(tmp, out, ATOMIC_MOVE, REPLACE_EXISTING);

        long outBytes = Files.size(out);
        log("COMPACT_WRITE", "out=%s bytes=%d inputBytes=%d writeAmplification=%.2f",
            out.getFileName(), outBytes, inputBytes,
            inputBytes == 0 ? 0.0 : (double) (inputBytes + outBytes) / (double) outBytes);

        // Update manifest: replace all input ssts with the new output sst
        manifest.replaceAllWith(outName);
        manifest.persistAtomically(dataDir.resolve("manifest.txt"));
        log("MANIFEST", "replaced inputs=%d with out=%s totalSst=%d",
            inputNames.size(), outName, manifest.sstableCount());

        // Delete old files (after manifest updated)
        int deleted = 0;
        for (Path p : inputPaths) {
            if (Files.deleteIfExists(p)) deleted++;
        }
        log("COMPACT_DONE", "deletedOld=%d", deleted);
    }

    private static void log(String tag, String fmt, Object... args) {
        System.out.print("[" + tag + "] ");
        System.out.printf(fmt + "%n", args);
    }
}

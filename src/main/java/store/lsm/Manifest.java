package store.lsm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.nio.file.StandardCopyOption.*;

/*
 * Manifest for LSM storage engine.
 */
public final class Manifest {

    private int nextSstId;
    private final List<String> sstables = new ArrayList<>();

    private Manifest() {}

    /* ---------- Load / Create ---------- */

    /**
     * Load an existing manifest or create a new one if it doesn't exist.
     * @param path Path to manifest file
     * 
     * @return Loaded or newly created Manifest
     * @throws IOException
     */
    public static Manifest loadOrCreate(Path path) throws IOException {
        Manifest m = new Manifest();

        if (!Files.exists(path)) {
            m.nextSstId = 1;
            m.persistAtomically(path);
            return m;
        }

        for (String line : Files.readAllLines(path)) {
            if (line.startsWith("nextSstId=")) {
                m.nextSstId = Integer.parseInt(line.substring(10));
            } else if (line.startsWith("sst=")) {
                m.sstables.add(line.substring(4));
            }
        }
        return m;
    }

    /* ---------- ID ---------- */

    /**
     * Get the next SSTable ID and increment the counter.
     * 
     * @return Next SSTable ID
     */
    public synchronized int nextId() {
        return nextSstId++;
    }

    /* ---------- SSTables ---------- */
    /**
     * Add a new SSTable to the manifest.
     * @param name SSTable name
     */
    public synchronized void addSstable(String name) {
        sstables.add(name);
    }

    /**
     * Replace a set of SSTables with a new one.
     * @param toRemove List of SSTable names to remove
     * @param toAdd New SSTable name to add
     */
    public synchronized void replaceSstables(List<String> toRemove, String toAdd) {
        sstables.removeAll(toRemove);
        sstables.add(toAdd);
    }

    /**
     * Get SSTables paths from newest to oldest.
     * @param sstDir Path to SSTable directory
     * 
     * @return List of SSTable paths from newest to oldest
     */
    public synchronized List<Path> sstablesNewestFirst(Path sstDir) {
        List<String> copy = new ArrayList<>(sstables);
        Collections.reverse(copy);

        List<Path> out = new ArrayList<>();
        for (String s : copy) out.add(sstDir.resolve(s));
        return out;
    }

    /**
     * Get the count of SSTables in the manifest.
     * @return Number of SSTables
     */
    public synchronized int sstableCount() {
        return sstables.size();
    }

    /**
     * Get SSTable names from oldest to newest.
     * 
     * @return List of SSTable names from oldest to newest
     */
    public synchronized List<String> sstablesOldestToNewest() {
        return new ArrayList<>(sstables);
    }

    /**
     * Replace all SSTables with a single new one.
     * @param outName
     */
    public synchronized void replaceAllWith(String outName) {
        sstables.clear();
        sstables.add(outName);
    }

    /* ---------- Persistence ---------- */

    /**
     * Persist the manifest atomically to the given path.
     * @param path Path to persist the manifest
     * 
     * @throws IOException
     */
    public synchronized void persistAtomically(Path path) throws IOException {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");

        List<String> lines = new ArrayList<>();
        lines.add("nextSstId=" + nextSstId);
        for (String sst : sstables) {
            lines.add("sst=" + sst);
        }

        Files.write(tmp, lines);
        Files.move(tmp, path, ATOMIC_MOVE, REPLACE_EXISTING);
    }
}
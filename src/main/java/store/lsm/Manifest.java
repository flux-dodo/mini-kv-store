package store.lsm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.nio.file.StandardCopyOption.*;

public final class Manifest {

    private int nextSstId;
    private final List<String> sstables = new ArrayList<>();

    private Manifest() {}

    /* ---------- Load / Create ---------- */

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

    public synchronized int nextId() {
        return nextSstId++;
    }

    /* ---------- SSTables ---------- */

    public synchronized void addSstable(String name) {
        sstables.add(name);
    }

    public synchronized void replaceSstables(List<String> toRemove, String toAdd) {
        sstables.removeAll(toRemove);
        sstables.add(toAdd);
    }

    public synchronized List<Path> sstablesNewestFirst(Path sstDir) {
        List<String> copy = new ArrayList<>(sstables);
        Collections.reverse(copy);

        List<Path> out = new ArrayList<>();
        for (String s : copy) out.add(sstDir.resolve(s));
        return out;
    }

    public synchronized int sstableCount() {
        return sstables.size();
    }

    /* ---------- Persistence ---------- */

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

    public synchronized List<String> sstablesOldestToNewest() {
        return new ArrayList<>(sstables);
    }

    public synchronized void replaceAllWith(String outName) {
        sstables.clear();
        sstables.add(outName);
    }
}
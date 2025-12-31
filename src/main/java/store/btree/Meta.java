package store.btree;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardCopyOption.*;

/**
 * Meta file for the B-Tree key-value store.
 */
public final class Meta {

    private int rootPageId;
    private int nextPageId;
    private int pageSize;
    private int version;
    private int magic;

    private static final int EXPECTED_MAGIC = 0xBEEFBEEF;

    private Meta() {}

    /**
     * Load an existing meta or create a new one if it doesn't exist.
     * @param path Path to meta file
     * 
     * @return Loaded or newly created Meta
     * @throws IOException
     */
    public static Meta loadOrCreate(Path path) throws IOException {
        Meta m = new Meta();

        if (!Files.exists(path)) {
            m.rootPageId = 0;     // Invariant: page 0 is the root and must be created before use
            m.nextPageId = 1;
            m.pageSize = 4096;
            m.version = 1;
            m.magic = 0xBEEFBEEF;

            m.persistAtomically(path);
            return m;
        }

        for (String line : Files.readAllLines(path)) {
            if (line.startsWith("rootPageId=")) {
                m.rootPageId = Integer.parseInt(line.substring(11));
            } else if (line.startsWith("nextPageId=")) {
                m.nextPageId = Integer.parseInt(line.substring(11));
            } else if (line.startsWith("pageSize=")) {
                m.pageSize = Integer.parseInt(line.substring(9));
            } else if (line.startsWith("version=")) {
                m.version = Integer.parseInt(line.substring(8));
            } else if (line.startsWith("magic=")) {
                m.magic = Integer.parseInt(line.substring(6));
                if (m.magic != EXPECTED_MAGIC) {
                    throw new IOException("Bad meta magic: " + m.magic);
                }
            } else {
                throw new IOException("Invalid meta line: " + line);
            }
        }
        return m;
    }

    /**
     * Allocate next Page ID and increment the counter.
     * 
     * @return Next Page ID
     */
    public synchronized int allocPageId() {
        return nextPageId++;
    }

    /**
     * Persist the meta atomically to the given path.
     * @param path Path to persist the meta
     * 
     * @throws IOException
     */
    public synchronized void persistAtomically(Path path) throws IOException {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");

        List<String> lines = new ArrayList<>();
        lines.add("rootPageId=" + rootPageId);
        lines.add("nextPageId=" + nextPageId);
        lines.add("pageSize=" + pageSize);
        lines.add("version=" + version);
        lines.add("magic=" + magic);

        Files.write(tmp, lines);
        Files.move(tmp, path, ATOMIC_MOVE, REPLACE_EXISTING);
    }
    
    /*  Getters - Setters */

    public synchronized int getRootPageId() {
        return rootPageId;
    }

    public synchronized void setRootPageId(int rootPageId) {
        this.rootPageId = rootPageId;
    }

    public synchronized int getPageSize() {
        return pageSize;
    }
}

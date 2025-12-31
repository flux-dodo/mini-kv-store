package store.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import store.KV;

/**
 * Mini B-Tree KV store with page-image WAL (redo-only).
 *
 * Commit protocol per put():
 *   1) Collect page images to write
 *   2) Append all to WAL
 *   3) WAL fsync
 *   4) Write pages to page file
 *   5) Page file fsync
 *   6) Persist meta
 *   7) Reset WAL
 */
public class MiniBTreeKV implements KV, AutoCloseable {

    private final PageFile pageFile;
    private final Meta meta;
    private final Path metaPath;
    private final Wal wal;

    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static final int MAX_KEYS_PER_PAGE = 3;

    public MiniBTreeKV(Path baseDir) throws IOException {
        Files.createDirectories(baseDir);

        this.metaPath = baseDir.resolve("meta.txt");
        this.meta = Meta.loadOrCreate(metaPath);

        this.wal = new Wal(baseDir.resolve("wal.log"));

        Path pagesPath = baseDir.resolve("btree.data");
        this.pageFile = new PageFile(pagesPath, DEFAULT_PAGE_SIZE);

        // --- Recovery: redo WAL into page file, then checkpoint (fsync + reset WAL) ---
        int redone = wal.replayInto(pageFile);
        if (redone > 0) {
            pageFile.fsync();
            wal.reset();
        }

        // --- Ensure root exists (fresh DB) ---
        if (!pageExists(0)) {
            Page root = new Page(0, true);
            ByteBuffer rootBytes = PageHelper.encode(root, DEFAULT_PAGE_SIZE);

            // write-ahead
            wal.appendPage(0, rootBytes, DEFAULT_PAGE_SIZE);
            wal.fsync();

            pageFile.writePage(0, rootBytes);
            pageFile.fsync();

            meta.setRootPageId(0);
            meta.persistAtomically(metaPath);

            wal.reset();
            System.out.println("Initialized new B-Tree with root page 0");
        }

        if (Boolean.getBoolean("wal.persist")) {
            System.out.println("[WAL] reset skipped (debug mode)");
        }
    }

    @Override
    public byte[] get(String key) throws IOException {
        if (key == null) throw new IllegalArgumentException("key is null");

        int pageId = meta.getRootPageId();
        while (true) {
            Page page = PageHelper.decode(pageId, pageFile.readPage(pageId));

            if (page.isLeaf) {
                int idx = page.findKeyIndex(key);
                return (idx >= 0) ? page.leafData.values.get(idx) : null;
            }

            int childIdx = page.childIndexFor(key);
            pageId = page.internalData.childPageIds.get(childIdx);
        }
    }

    @Override
    public void put(String key, byte[] value) throws IOException {
        if (key == null) throw new IllegalArgumentException("key is null");
        if (value == null) throw new IllegalArgumentException("value is null");

        Stack<PathEntry> path = new Stack<>();
        List<PageWrite> batch = new ArrayList<>();

        int pageId = meta.getRootPageId();

        // descend
        while (true) {
            Page page = PageHelper.decode(pageId, pageFile.readPage(pageId));

            if (page.isLeaf) {
                page.insertKey(key, value);

                // leaf always gets written
                batch.add(new PageWrite(page.pageId, PageHelper.encode(page, DEFAULT_PAGE_SIZE)));

                Promotion promo = page.isFull(MAX_KEYS_PER_PAGE) ? leafSplit(page, batch) : null;
                promo = propagateSplit(promo, path, batch);

                // --- COMMIT (single WAL fsync + single pageFile fsync) ---
                for (PageWrite w : batch) wal.appendPage(w.pageId, w.bytes, DEFAULT_PAGE_SIZE);
                wal.fsync();

                for (PageWrite w : batch) pageFile.writePage(w.pageId, w.bytes);
                pageFile.fsync();

                meta.persistAtomically(metaPath);
                
                // For forcing recovery and debugging, run with flag -Dwal.persist=true
                if (!Boolean.getBoolean("wal.persist")) {
                    wal.reset();
                }

                return;
            }

            int childIdx = page.childIndexFor(key);
            path.push(new PathEntry(page.pageId, childIdx));
            pageId = page.internalData.childPageIds.get(childIdx);
        }
    }

    private Promotion propagateSplit(Promotion promo, Stack<PathEntry> path, List<PageWrite> batch) throws IOException {
        while (promo != null && !path.isEmpty()) {
            PathEntry e = path.pop();
            Page parent = PageHelper.decode(e.pageId, pageFile.readPage(e.pageId));

            parent.internalData.keys.add(e.childIdx, promo.key);
            parent.internalData.childPageIds.add(e.childIdx + 1, promo.rightPageId);

            // parent updated -> write it
            batch.add(new PageWrite(parent.pageId, PageHelper.encode(parent, DEFAULT_PAGE_SIZE)));

            promo = parent.isFull(MAX_KEYS_PER_PAGE) ? internalSplit(parent, batch) : null;
        }

        if (promo != null) {
            // root split -> create new root
            Page newRoot = new Page(meta.allocPageId(), false);
            newRoot.internalData.keys.add(promo.key);
            newRoot.internalData.childPageIds.add(meta.getRootPageId());
            newRoot.internalData.childPageIds.add(promo.rightPageId);

            meta.setRootPageId(newRoot.pageId);
            System.out.printf("Root split: new root pageId=%d%n", newRoot.pageId);

            batch.add(new PageWrite(newRoot.pageId, PageHelper.encode(newRoot, DEFAULT_PAGE_SIZE)));
        }

        return promo;
    }

    private Promotion leafSplit(Page page, List<PageWrite> batch) throws IOException {
        int mid = page.leafData.keys.size() / 2;

        Page right = new Page(meta.allocPageId(), true);

        // move mid..end to right
        for (int i = mid; i < page.leafData.keys.size(); i++) {
            right.insertKey(page.leafData.keys.get(i), page.leafData.values.get(i));
        }

        // trim left to < mid
        while (page.leafData.keys.size() > mid) {
            page.leafData.keys.remove(page.leafData.keys.size() - 1);
            page.leafData.values.remove(page.leafData.values.size() - 1);
        }

        // Write both pages (updated left + new right)
        batch.add(new PageWrite(page.pageId, PageHelper.encode(page, DEFAULT_PAGE_SIZE)));
        batch.add(new PageWrite(right.pageId, PageHelper.encode(right, DEFAULT_PAGE_SIZE)));

        // promote smallest key in right
        Promotion promo = new Promotion(right.leafData.keys.get(0), right.pageId);
        System.out.printf("Leaf split: promote key='%s' to parent%n", promo.key);
        return promo;
    }

    private Promotion internalSplit(Page page, List<PageWrite> batch) throws IOException {
        int mid = page.internalData.keys.size() / 2;
        String promote = page.internalData.keys.get(mid);

        Page right = new Page(meta.allocPageId(), false);

        // right keys: mid+1..end
        for (int i = mid + 1; i < page.internalData.keys.size(); i++) {
            right.internalData.keys.add(page.internalData.keys.get(i));
        }

        // right children: mid+1..end (children count = keys+1)
        for (int i = mid + 1; i < page.internalData.childPageIds.size(); i++) {
            right.internalData.childPageIds.add(page.internalData.childPageIds.get(i));
        }

        // trim left keys to 0..mid-1 (drop promoted key)
        while (page.internalData.keys.size() > mid) {
            page.internalData.keys.remove(page.internalData.keys.size() - 1);
        }

        // trim left children to 0..mid (keep mid+1 children)
        while (page.internalData.childPageIds.size() > mid + 1) {
            page.internalData.childPageIds.remove(page.internalData.childPageIds.size() - 1);
        }

        batch.add(new PageWrite(page.pageId, PageHelper.encode(page, DEFAULT_PAGE_SIZE)));
        batch.add(new PageWrite(right.pageId, PageHelper.encode(right, DEFAULT_PAGE_SIZE)));

        System.out.printf("Internal split: promote key='%s' to parent%n", promote);
        return new Promotion(promote, right.pageId);
    }

    @Override
    public void delete(String key) {
        throw new UnsupportedOperationException();
    }

    private boolean pageExists(int pageId) {
        try {
            pageFile.readPage(pageId);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        pageFile.close();
    }

    record PathEntry(int pageId, int childIdx) {}
    record Promotion(String key, int rightPageId) {}
    record PageWrite(int pageId, ByteBuffer bytes) {}
}

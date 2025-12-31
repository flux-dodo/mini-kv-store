package store.lsm;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

/**
 * Helper class for SSTable operations in LSM store.
 */
public final class SstHelper {

    // Footer layout: [indexOffset: int64][indexCount: int32][magic: int32]
    private static final int FOOTER_BYTES = 8 + 4 + 4;

    /**
     * A magic number is a file-format signature used to detect corruption, wrong files, 
     * and incomplete writes early and reliably.
    */
    private static final int MAGIC = 0x5A7A0B1E; 

    private SstHelper() {}

    /* ----------------------------- Public API ----------------------------- */

    /**
     * Writes an SSTable to {@code outTmp} from sorted entries.
     * File format:
     *   Data section: repeated [kLen:int32][vLen:int32][kBytes][vBytes?]
     *   Index section: sparse entries every {@code sparseEvery} records:
     *       [kLen:int32][kBytes][offset:int64]
     *   Footer: [indexOffset:int64][indexCount:int32][magic:int32]
     */
    public static void write(Path outTmp,
                             NavigableMap<String, MemTable.ValueRecord> entriesSorted,
                             int sparseEvery) throws IOException {

        if (sparseEvery <= 0) throw new IllegalArgumentException("sparseEvery must be > 0");

        try (FileChannel ch = FileChannel.open(outTmp,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {

            List<IndexEntry> index = new ArrayList<>();
            long offset = 0;
            int i = 0;

            for (var e : entriesSorted.entrySet()) {
                String key = e.getKey();
                MemTable.ValueRecord vr = e.getValue();

                byte[] k = key.getBytes(StandardCharsets.UTF_8);

                int vLen;
                byte[] vBytes = null;
                if (vr == null || vr.tombstone()) {
                    vLen = -1;
                } else {
                    vBytes = vr.value();
                    if (vBytes == null) vLen = -1; // defensive
                    else vLen = vBytes.length;
                }

                // Sparse index points into the start of THIS record in data section.
                if (i % sparseEvery == 0) {
                    index.add(new IndexEntry(key, offset));
                }

                // Write record
                offset += writeRecord(ch, k, vBytes, vLen);

                i++;
            }

            long indexOffset = offset;

            // Write index section
            for (IndexEntry ie : index) {
                byte[] k = ie.key.getBytes(StandardCharsets.UTF_8);
                ByteBuffer buf = ByteBuffer.allocate(4 + k.length + 8).order(ByteOrder.BIG_ENDIAN);
                buf.putInt(k.length);
                buf.put(k);
                buf.putLong(ie.offset);
                buf.flip();
                while (buf.hasRemaining()) ch.write(buf);
                offset += (4 + k.length + 8);
            }

            // Write footer
            ByteBuffer footer = ByteBuffer.allocate(FOOTER_BYTES).order(ByteOrder.BIG_ENDIAN);
            footer.putLong(indexOffset);
            footer.putInt(index.size());
            footer.putInt(MAGIC);
            footer.flip();
            while (footer.hasRemaining()) ch.write(footer);

            ch.force(true);
        }
    }

    /**
     * Returns the latest value record for {@code key} in this SSTable, or null if absent.
     * If the returned record has tombstone=true, treat it as deleted.
     */
    public static MemTable.ValueRecord get(Path sstPath, String key) throws IOException {
        try (FileChannel ch = FileChannel.open(sstPath, StandardOpenOption.READ)) {
            Footer footer = readFooter(ch);
            List<IndexEntry> index = readIndex(ch, footer);

            long startOffset = findStartOffset(index, key);
            return scanFromOffset(ch, startOffset, footer.indexOffset, key);
        }
    }

    /**
     * Reads all entries from the SSTable into memory.
     * Used for compaction.
     */
    public static NavigableMap<String, MemTable.ValueRecord> readAll(Path sstPath) throws IOException {
        NavigableMap<String, MemTable.ValueRecord> result = new java.util.TreeMap<>();

        try (FileChannel ch = FileChannel.open(sstPath, StandardOpenOption.READ)) {
            Footer footer = readFooter(ch);

            long pos = 0;
            ByteBuffer hdr = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);

            while (pos < footer.indexOffset) {

                // Ensure we can read header fully without crossing into index section
                if (pos + 8 > footer.indexOffset) {
                    throw new IOException("Corrupt SST: record header crosses into index section");
                }

                hdr.clear();
                readFully(ch, hdr, pos);
                hdr.flip();
                int kLen = hdr.getInt();
                int vLen = hdr.getInt();
                pos += 8;

                if (kLen <= 0 || kLen > 10_000_000) throw new IOException("Bad kLen=" + kLen);

                // Ensure key bytes are fully within data section
                if (pos + kLen > footer.indexOffset) {
                    throw new IOException("Corrupt SST: key crosses into index section");
                }

                byte[] kBytes = new byte[kLen];
                readFully(ch, ByteBuffer.wrap(kBytes), pos);
                pos += kLen;

                String key = new String(kBytes, StandardCharsets.UTF_8);

                if (vLen == -1) {
                    result.put(key, new MemTable.ValueRecord(null, true));
                    continue;
                }

                if (vLen < 0 || vLen > 100_000_000) throw new IOException("Bad vLen=" + vLen);

                // Ensure value bytes are fully within data section
                if (pos + vLen > footer.indexOffset) {
                    throw new IOException("Corrupt SST: value crosses into index section");
                }

                byte[] vBytes = new byte[vLen];
                readFully(ch, ByteBuffer.wrap(vBytes), pos);
                pos += vLen;

                result.put(key, new MemTable.ValueRecord(vBytes, false));
            }
        }

        return result;
    }


    /* ----------------------------- Internals ----------------------------- */

    private static long writeRecord(FileChannel ch, byte[] kBytes, byte[] vBytes, int vLen) throws IOException {
        int recordBytes = 4 + 4 + kBytes.length + (vLen == -1 ? 0 : vLen);

        ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        header.putInt(kBytes.length);
        header.putInt(vLen);
        header.flip();
        while (header.hasRemaining()) ch.write(header);

        ch.write(ByteBuffer.wrap(kBytes));

        if (vLen != -1) {
            ch.write(ByteBuffer.wrap(vBytes));
        }

        return recordBytes;
    }

    private static Footer readFooter(FileChannel ch) throws IOException {
        long size = ch.size();
        if (size < FOOTER_BYTES) throw new IOException("SST too small for footer: " + size);

        ByteBuffer footer = ByteBuffer.allocate(FOOTER_BYTES).order(ByteOrder.BIG_ENDIAN);
        readFully(ch, footer, size - FOOTER_BYTES);
        footer.flip();

        long indexOffset = footer.getLong();
        int indexCount = footer.getInt();
        int magic = footer.getInt();

        if (magic != MAGIC) throw new IOException("Bad SST magic. Expected=" + MAGIC + " got=" + magic);
        if (indexOffset < 0 || indexOffset > size - FOOTER_BYTES) throw new IOException("Bad indexOffset=" + indexOffset);
        if (indexCount < 0) throw new IOException("Bad indexCount=" + indexCount);

        return new Footer(indexOffset, indexCount);
    }

    private static List<IndexEntry> readIndex(FileChannel ch, Footer footer) throws IOException {
        List<IndexEntry> idx = new ArrayList<>(footer.indexCount);

        long pos = footer.indexOffset;
        for (int i = 0; i < footer.indexCount; i++) {
            // [kLen:int32]
            ByteBuffer b4 = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
            readFully(ch, b4, pos);
            b4.flip();
            int kLen = b4.getInt();
            pos += 4;

            if (kLen <= 0 || kLen > 1_000_000) throw new IOException("Bad index key length: " + kLen);

            byte[] kBytes = new byte[kLen];
            readFully(ch, ByteBuffer.wrap(kBytes), pos);
            pos += kLen;

            // [offset:int64]
            ByteBuffer b8 = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
            readFully(ch, b8, pos);
            b8.flip();
            long off = b8.getLong();
            pos += 8;

            idx.add(new IndexEntry(new String(kBytes, StandardCharsets.UTF_8), off));
        }

        return idx;
    }

    // Find the greatest index entry key <= targetKey; return its offset. If none, start at 0.
    private static long findStartOffset(List<IndexEntry> index, String targetKey) {
        if (index.isEmpty()) return 0;

        int lo = 0, hi = index.size() - 1;
        int best = -1;

        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = index.get(mid).key.compareTo(targetKey);
            if (cmp <= 0) {
                best = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        return (best == -1) ? 0 : index.get(best).offset;
    }

    private static MemTable.ValueRecord scanFromOffset(FileChannel ch,
                                                      long startOffset,
                                                      long dataEndOffset,
                                                      String targetKey) throws IOException {

        long pos = startOffset;

        while (pos < dataEndOffset) {
            // Read header: [kLen][vLen]
            ByteBuffer hdr = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
            readFully(ch, hdr, pos);
            hdr.flip();
            int kLen = hdr.getInt();
            int vLen = hdr.getInt();
            pos += 8;

            if (kLen <= 0 || kLen > 10_000_000) throw new IOException("Bad kLen=" + kLen);

            byte[] kBytes = new byte[kLen];
            readFully(ch, ByteBuffer.wrap(kBytes), pos);
            pos += kLen;

            String key = new String(kBytes, StandardCharsets.UTF_8);

            int cmp = key.compareTo(targetKey);

            if (vLen == -1) {
                // tombstone record
                if (cmp == 0) return new MemTable.ValueRecord(null, true);
                if (cmp > 0) return null; // passed target key
                continue;
            }

            if (vLen < 0 || vLen > 100_000_000) throw new IOException("Bad vLen=" + vLen);

            if (cmp == 0) {
                byte[] v = new byte[vLen];
                readFully(ch, ByteBuffer.wrap(v), pos);
                return new MemTable.ValueRecord(v, false);
            } else {
                // Skip value bytes
                pos += vLen;
                if (cmp > 0) return null; // passed target key
            }
        }

        return null;
    }

    private static void readFully(FileChannel ch, ByteBuffer dst, long position) throws IOException {
        dst.clear();
        int read;
        long pos = position;
        while (dst.hasRemaining()) {
            read = ch.read(dst, pos);
            if (read < 0) throw new EOFException("Unexpected EOF");
            pos += read;
        }
    }

    /* ----------------------------- Small records ----------------------------- */

    private record IndexEntry(String key, long offset) {}
    private record Footer(long indexOffset, int indexCount) {}
}

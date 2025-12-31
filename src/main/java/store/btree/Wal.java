package store.btree;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * Redo-only page-image WAL for B-Tree durability.
 *
 * Record format (big-endian):
 *   [pageId:int32][pageSize:int32][pageBytes:pageSize][crc32:int32]
 *
 * - pageSize is included so the WAL is self-describing.
 * - crc32 covers (pageId, pageSize, pageBytes) to detect torn/corrupt tail record.
 */
public final class Wal {
    private static final ByteOrder BO = ByteOrder.BIG_ENDIAN;
    private static final int HDR_BYTES = 4 + 4;     // pageId + pageSize
    private static final int CRC_BYTES = 4;

    private final Path path;

    public Wal(Path path) {
        this.path = path;
    }

    /** Append a full page image. Caller should pass a buffer with exactly pageSize bytes. */
    public synchronized void appendPage(int pageId, ByteBuffer pageBytes, int pageSize) throws IOException {
        if (pageId < 0) throw new IllegalArgumentException("pageId must be >= 0");
        if (pageBytes == null) throw new IllegalArgumentException("pageBytes is null");

        // Normalize to full-page view: position=0, limit=pageSize.
        ByteBuffer dup = pageBytes.duplicate().order(BO);
        dup.position(0);
        dup.limit(pageSize);

        int crc = crc32(pageId, pageSize, dup);

        try (FileChannel ch = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {

            ByteBuffer hdr = ByteBuffer.allocate(HDR_BYTES).order(BO);
            hdr.putInt(pageId);
            hdr.putInt(pageSize);
            hdr.flip();
            ch.write(hdr);

            ch.write(dup);

            ByteBuffer c = ByteBuffer.allocate(CRC_BYTES).order(BO);
            c.putInt(crc).flip();
            ch.write(c);
        }
    }

    /** fsync WAL (durability point). Call before applying pages to btree.data. */
    public synchronized void fsync() throws IOException {
        if (!Files.exists(path)) return;
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ch.force(true);
        }
    }

    /**
     * Replay valid WAL records into PageFile.
     * Stops cleanly if it finds a torn tail record.
     */
    public int replayInto(PageFile pageFile) throws IOException {
        if (!Files.exists(path)) return 0;

        int applied = 0;

        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            long size = ch.size();
            long pos = 0;

            while (pos < size) {
                if (pos + HDR_BYTES > size) break; // torn header

                ByteBuffer hdr = ByteBuffer.allocate(HDR_BYTES).order(BO);
                readFully(ch, hdr, pos);
                hdr.flip();

                int pageId = hdr.getInt();
                int pageSize = hdr.getInt();
                pos += HDR_BYTES;

                if (pageId < 0) throw new IOException("Corrupt WAL: pageId=" + pageId);
                if (pageSize <= 0 || pageSize > 1_000_000) throw new IOException("Corrupt WAL: pageSize=" + pageSize);

                long need = (long) pageSize + CRC_BYTES;
                if (pos + need > size) break; // torn payload

                ByteBuffer payload = ByteBuffer.allocate(pageSize).order(BO);
                readFully(ch, payload, pos);
                payload.flip();
                pos += pageSize;

                ByteBuffer c = ByteBuffer.allocate(CRC_BYTES).order(BO);
                readFully(ch, c, pos);
                c.flip();
                int expected = c.getInt();
                pos += CRC_BYTES;

                int actual = crc32(pageId, pageSize, payload.duplicate().position(0));
                if (actual != expected) break; // corrupt/torn tail

                // Apply redo
                pageFile.writePage(pageId, payload);
                applied++;
            }
        }
        System.out.printf("WAL replay applied %d pages%n", applied);
        return applied;
    }

    /** Truncate WAL after successful checkpoint (pages+meta safely persisted). */
    public synchronized void reset() throws IOException {
        Files.write(path, new byte[0],
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static void readFully(FileChannel ch, ByteBuffer dst, long position) throws IOException {
        long pos = position;
        while (dst.hasRemaining()) {
            int n = ch.read(dst, pos);
            if (n < 0) throw new EOFException("Unexpected EOF");
            if (n == 0) throw new IOException("Stuck read");
            pos += n;
        }
    }

    private static int crc32(int pageId, int pageSize, ByteBuffer payload) {
        CRC32 crc = new CRC32();

        ByteBuffer tmp = ByteBuffer.allocate(8).order(BO);
        tmp.putInt(pageId).putInt(pageSize).flip();
        crc.update(tmp);

        ByteBuffer p = payload.duplicate();
        byte[] buf = new byte[Math.min(8192, p.remaining())];
        while (p.hasRemaining()) {
            int n = Math.min(buf.length, p.remaining());
            p.get(buf, 0, n);
            crc.update(buf, 0, n);
        }
        return (int) crc.getValue();
    }
}

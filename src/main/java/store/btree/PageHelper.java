package store.btree;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * PageCodec for B-Tree pages.
 *
 * Fixed-size page layout: [header][payload][padding]
 *
 * Header (32 bytes):
 *   [magic:int32][version:int32][flags:int32][keyCount:int32]
 *   [reserved:16 bytes]
 *
 * flags:
 *   bit0 = isLeaf (1 = leaf, 0 = internal)
 *
 * Leaf payload (repeated keyCount times):
 *   [kLen:int32][kBytes][vLen:int32][vBytes]
 *
 * Internal payload:
 *   [child0:int32]
 *   repeat keyCount times:
 *     [kLen:int32][kBytes][child(i+1):int32]
 *
 * Invariant: internal.childPageIds.size() == internal.keys.size() + 1
 */
public final class PageHelper {
    public static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    private static final int MAGIC = 0xDEADBEEF;
    private static final int SUPPORTED_VERSION = 1;

    // 32-byte header
    private static final int HEADER_SIZE = 32;
    private static final int RESERVED_SIZE = HEADER_SIZE - (4 + 4 + 4 + 4);

    // flags bits
    private static final int FLAG_LEAF = 1 << 0;

    public PageHelper() {}

    public static Page decode(int pageId, ByteBuffer pageBytes) {
        if (pageBytes == null) throw new IllegalArgumentException("pageBytes is null");
        if (pageBytes.capacity() < HEADER_SIZE) throw new IllegalArgumentException("Page too small for header");
        pageBytes.order(BYTE_ORDER);

        // Always decode from start of page
        pageBytes.position(0);

        int magic = pageBytes.getInt();
        if (magic != MAGIC) throw new IllegalArgumentException("Bad page magic: " + magic);

        int version = pageBytes.getInt();
        if (version != SUPPORTED_VERSION) throw new IllegalArgumentException("Unsupported page version: " + version);

        int flags = pageBytes.getInt();
        boolean isLeaf = (flags & FLAG_LEAF) != 0;

        int keyCount = pageBytes.getInt();
        if (keyCount < 0) throw new IllegalArgumentException("Bad keyCount: " + keyCount);

        // skip reserved
        pageBytes.position(pageBytes.position() + RESERVED_SIZE);

        Page page = new Page(pageId, isLeaf);

        if (isLeaf) {
            for (int i = 0; i < keyCount; i++) {
                int kLen = pageBytes.getInt();
                if (kLen <= 0 || kLen > 10_000_000) throw new IllegalArgumentException("Bad kLen=" + kLen);
                requireRemaining(pageBytes, kLen);

                byte[] kBytes = new byte[kLen];
                pageBytes.get(kBytes);
                String key = new String(kBytes, StandardCharsets.UTF_8);

                int vLen = pageBytes.getInt();
                if (vLen < 0 || vLen > 100_000_000) throw new IllegalArgumentException("Bad vLen=" + vLen);
                requireRemaining(pageBytes, vLen);

                byte[] vBytes = new byte[vLen];
                pageBytes.get(vBytes);

                page.leafData.keys.add(key);
                page.leafData.values.add(vBytes);
            }
        } else {
            // internal: child0 first
            if (keyCount == 0) {
                // Still may have 1 child pointer in some implementations, but for now treat empty internal as invalid.
                // You can relax this later if you want.
                // requireRemaining(pageBytes, 4);
                // page.internalData.childPageIds.add(pageBytes.getInt());
                return page;
            }

            requireRemaining(pageBytes, 4);
            page.internalData.childPageIds.add(pageBytes.getInt());

            for (int i = 0; i < keyCount; i++) {
                int kLen = pageBytes.getInt();
                if (kLen <= 0 || kLen > 10_000_000) throw new IllegalArgumentException("Bad kLen=" + kLen);
                requireRemaining(pageBytes, kLen);

                byte[] kBytes = new byte[kLen];
                pageBytes.get(kBytes);
                String key = new String(kBytes, StandardCharsets.UTF_8);

                requireRemaining(pageBytes, 4);
                int child = pageBytes.getInt();

                page.internalData.keys.add(key);
                page.internalData.childPageIds.add(child);
            }

            // Invariant: children = keys + 1
            if (page.internalData.childPageIds.size() != page.internalData.keys.size() + 1) {
                throw new IllegalArgumentException(
                        "Corrupt internal page: children=" + page.internalData.childPageIds.size() +
                        " keys=" + page.internalData.keys.size());
            }
        }

        // Leave buffer position unspecified for caller; they should treat it as page blob.
        return page;
    }

    public static ByteBuffer encode(Page page, int pageSize) {
        if (page == null) throw new IllegalArgumentException("page is null");
        if (pageSize < HEADER_SIZE) throw new IllegalArgumentException("pageSize too small");
        ByteBuffer buf = ByteBuffer.allocate(pageSize).order(BYTE_ORDER);

        int flags = 0;
        if (page.isLeaf) flags |= FLAG_LEAF;

        int keyCount = page.isLeaf ? page.leafData.keys.size() : page.internalData.keys.size();
        if (keyCount < 0) throw new IllegalArgumentException("Bad keyCount=" + keyCount);

        // header
        buf.putInt(MAGIC);
        buf.putInt(SUPPORTED_VERSION);
        buf.putInt(flags);
        buf.putInt(keyCount);
        for (int i = 0; i < RESERVED_SIZE; i++) buf.put((byte) 0);

        // payload
        if (page.isLeaf) {
            if (page.leafData.values.size() != keyCount) {
                throw new IllegalArgumentException("Leaf invariant violated: keys != values");
            }

            for (int i = 0; i < keyCount; i++) {
                String key = page.leafData.keys.get(i);
                byte[] kBytes = key.getBytes(StandardCharsets.UTF_8);

                byte[] vBytes = page.leafData.values.get(i);
                if (vBytes == null) throw new IllegalArgumentException("Leaf value is null at i=" + i);

                putIntChecked(buf, kBytes.length, pageSize);
                putBytesChecked(buf, kBytes, pageSize);

                putIntChecked(buf, vBytes.length, pageSize);
                putBytesChecked(buf, vBytes, pageSize);
            }
        } else {
            // internal invariant: children = keys + 1 (unless you allow empty internal nodes)
            int childCount = page.internalData.childPageIds.size();
            if (keyCount > 0 && childCount != keyCount + 1) {
                throw new IllegalArgumentException("Internal invariant violated: children=" + childCount + " keys=" + keyCount);
            }

            if (keyCount > 0) {
                // child0
                putIntChecked(buf, page.internalData.childPageIds.get(0), pageSize);

                for (int i = 0; i < keyCount; i++) {
                    String key = page.internalData.keys.get(i);
                    byte[] kBytes = key.getBytes(StandardCharsets.UTF_8);

                    putIntChecked(buf, kBytes.length, pageSize);
                    putBytesChecked(buf, kBytes, pageSize);

                    int child = page.internalData.childPageIds.get(i + 1);
                    putIntChecked(buf, child, pageSize);
                }
            }
        }

        // Remaining bytes are padding (zeros) by default in a newly allocated ByteBuffer.

        // Prepare for writing: writePage expects remaining == pageSize
        buf.position(0);
        buf.limit(pageSize);
        return buf;
    }

    private static void requireRemaining(ByteBuffer b, int n) {
        if (n < 0) throw new IllegalArgumentException("Negative length: " + n);
        if (b.remaining() < n) throw new IllegalArgumentException("Corrupt page: need " + n + " bytes, remaining " + b.remaining());
    }

    private static void putIntChecked(ByteBuffer b, int v, int pageSize) {
        if (b.position() + 4 > pageSize) throw new IllegalStateException("Page overflow");
        b.putInt(v);
    }

    private static void putBytesChecked(ByteBuffer b, byte[] bytes, int pageSize) {
        if (b.position() + bytes.length > pageSize) throw new IllegalStateException("Page overflow");
        b.put(bytes);
    }
}
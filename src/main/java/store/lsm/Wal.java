package store.lsm;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Write-Ahead Log (WAL) for durability.
 */
public final class Wal {
  private final Path path;

  public Wal(Path path) { this.path = path; }

  /**
   * Append a PUT record to the WAL.
   * @param key the key to put
   * @param value the value to associate with the key
   * 
   * @throws IOException
   */
  public synchronized void appendPut(String key, byte[] value) throws IOException {
    appendRecord(key, value, false);
  }

  /**
   * Append a DELETE record to the WAL.
   * @param key the key to delete
   * 
   * @throws IOException
   */
  public synchronized void appendDelete(String key) throws IOException {
    appendRecord(key, null, true);
  }

  /**
   * Replay WAL records into the given MemTable.
   * @param mem target MemTable
   * 
   * @return number of records applied
   */
  public int replayInto(MemTable mem) throws IOException {
    if (!Files.exists(path)) return 0;

    int applied = 0;
    long bytesRead = 0;

    try (var ch = FileChannel.open(path, StandardOpenOption.READ)) {
      long size = ch.size();
      long pos = 0;

      ByteBuffer hdr = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);

      while (pos < size) {
        // If we can't read a full header, treat remaining bytes as a torn record and stop.
        if (pos + 8 > size) {
          System.out.printf("[WAL_RECOVERY] Truncated header at pos=%d (fileSize=%d). Ignoring tail.%n", pos, size);
          break;
        }

        hdr.clear();
        readFully(ch, hdr, pos);
        hdr.flip();
        int keyLen = hdr.getInt();
        int valLen = hdr.getInt();
        pos += 8;

        // Basic sanity checks (prevents absurd allocations)
        if (keyLen <= 0 || keyLen > 10_000_000) {
          throw new IOException("Corrupt WAL: bad keyLen=" + keyLen + " at pos=" + (pos - 8));
        }
        if (valLen < -1 || valLen > 100_000_000) {
          throw new IOException("Corrupt WAL: bad valLen=" + valLen + " at pos=" + (pos - 4));
        }

        // If key bytes aren't fully present, stop (torn record)
        if (pos + keyLen > size) {
          System.out.printf("[WAL_RECOVERY] Truncated key at pos=%d keyLen=%d (fileSize=%d). Ignoring tail.%n",
              pos, keyLen, size);
          break;
        }

        byte[] k = readBytesAt(ch, pos, keyLen);
        String key = new String(k, StandardCharsets.UTF_8);
        pos += keyLen;

        if (valLen == -1) {
          mem.delete(key);
          applied++;
          continue;
        }

        // If value bytes aren't fully present, stop (torn record)
        if (pos + valLen > size) {
          System.out.printf("[WAL_RECOVERY] Truncated value at pos=%d valLen=%d (fileSize=%d). Ignoring tail.%n",
              pos, valLen, size);
          break;
        }

        byte[] v = readBytesAt(ch, pos, valLen);
        pos += valLen;

        mem.put(key, v);
        applied++;
      }

      bytesRead = pos;
    }

    System.out.printf("[WAL_RECOVERY] Applied=%d bytesConsumed=%d%n", applied, bytesRead);
    return applied;
  }

  /**
   * Reset (clear) the WAL.
   * 
   * @throws IOException
   */
  public void reset() throws IOException {
    // simplest: truncate
    Files.write(path, new byte[0], StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
  }

  private void appendRecord(String key, byte[] value, boolean tombstone) throws IOException {
    byte[] k = key.getBytes(StandardCharsets.UTF_8);
    int keyLen = k.length;
    int valLen = tombstone ? -1 : value.length;

    try (var ch = FileChannel.open(path,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {

      ByteBuffer header = ByteBuffer.allocate(8);
      header.putInt(keyLen).putInt(valLen).flip();
      ch.write(header);
      ch.write(ByteBuffer.wrap(k));
      if (!tombstone) ch.write(ByteBuffer.wrap(value));

      ch.force(true); // durability (can batch later)
    }
  }

  /** Reads exactly dst.remaining() bytes from absolute file position. */
  private static void readFully(FileChannel ch, ByteBuffer dst, long position) throws IOException {
    long pos = position;
    while (dst.hasRemaining()) {
      int n = ch.read(dst, pos);
      if (n < 0) throw new EOFException("Unexpected EOF while reading WAL");
      pos += n;
    }
  }

  private static byte[] readBytesAt(FileChannel ch, long position, int n) throws IOException {
    ByteBuffer b = ByteBuffer.allocate(n);
    readFully(ch, b, position);
    return b.array();
  }
}


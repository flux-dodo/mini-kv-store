package store.btree;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PageFile implements AutoCloseable {
    private final int pageSize;
    private final FileChannel ch;

    public PageFile(Path filePath, int pageSize) throws IOException{
        this.pageSize = pageSize;

        this.ch = FileChannel.open(filePath,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE);
    }

    /**
     * Reads a page from the file for the specified pageId.
     * @param pageId the ID of the page to read
     * 
     * @return the ByteBuffer containing the page data
     * @throws IOException
     */
    public ByteBuffer readPage(int pageId) throws IOException {
        if (pageId < 0) throw new IllegalArgumentException("Bad input: pageId must be >= 0");

        ByteBuffer buffer = ByteBuffer.allocate(pageSize).order(PageHelper.BYTE_ORDER);

        long offset = offsetOf(pageId);
        while (buffer.hasRemaining()) {
            int read = ch.read(buffer, offset);
            if (read < 0) throw new EOFException("Page does not exist: " + pageId);
            if (read == 0) throw new IOException("Stuck read");
            offset += read;
        }
        buffer.rewind();
        return buffer;
    }

    /**
     * Writes a page to the file for the specified pageId.
     * @param pageId the ID of the page to write
     * @param data the data to write
     * 
     * @throws IOException
     */
    public void writePage(int pageId, ByteBuffer data) throws IOException {
        if (pageId < 0) throw new IllegalArgumentException("pageId must be >= 0");
        if (data.capacity() != pageSize) throw new IllegalArgumentException("Data capacity must equal pageSize");

        long offset = offsetOf(pageId);

        data.rewind();
        data.limit(pageSize);
        if (data.remaining() != pageSize) throw new IllegalArgumentException("Buffer must contain full page");

        while (data.hasRemaining()) {
            int written = ch.write(data, offset);
            if(written <= 0) throw new IOException("Failed to write data");
            offset += written;
        }
    }

    /**
     * Forces any updates to be written to the storage device.
     * 
     * @throws IOException
     */
    public void fsync() throws IOException {
        ch.force(true);
    }

    @Override
    public void close() throws IOException {
        ch.close();
    }

    private long offsetOf(int pageId) { return (long) pageId * pageSize; }
}

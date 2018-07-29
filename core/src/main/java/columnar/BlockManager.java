/**
 * 
 */
package columnar;

import java.io.IOException;

import org.apache.trevni.Input;

/**
 * @author Michael
 *
 */
public class BlockManager {
    public static final boolean TRACE_IO = true;
    public static final boolean AIO_OPEN = true;
    public static final int DEFAULT_SCALE = 32;
    public static final int MAX_FETCH_SIZE = 256 * 1024;
    private final int blockSize;
    private final int cacheScale;
    private final int columnNumber;
    private final byte[][] columnBuffer;
    private final int bufferSize;
    //private Input in; // Need to be encapsulated.
    private int totalRead = 0;
    private long ioTime = 0;
    private long readLength = 0;
    public long colBlockTime = 0;
    public long colStartTime = 0;
    public int headerIOs = 0;

    public BlockManager(int bs, int cs) {
        this(bs, cs, 0);
    }

    public BlockManager(int bs) {
        this(bs, DEFAULT_SCALE);
    }

    public BlockManager(int bs, int cs, int col) {
        this.blockSize = bs * 1024;
        this.cacheScale = cs;
        this.columnNumber = col;
        this.columnBuffer = new byte[columnNumber][];
        this.bufferSize = (blockSize * cacheScale > MAX_FETCH_SIZE) ? MAX_FETCH_SIZE : blockSize * cacheScale;
        for (int i = 0; i < columnNumber; i++) {
            //columnBuffer[i] = new byte[bufferSize];
        }
        GlobalInformation.totalBlockManagerCreated++;
    }

    public int fetch(final Input in, long offset, byte[] b, int start, int len) throws IOException {
        return read(in, offset, b, start, len);
    }

    public int read(final Input in, long offset, byte[] b, int start, int len) throws IOException {
        //System.out.println(offset + "+" + len + "=" + (offset + len));
        int readlen;
        if (TRACE_IO) {
            long begin = System.nanoTime();
            readlen = in.read(offset, b, start, len);
            ioTime += (System.nanoTime() - begin);
            totalRead++;
            readLength += readlen;
        } else {
            readlen = in.read(offset, b, start, len);
        }
        return readlen;
    }

    public void getBlock() {
        //this.in = new InputBuffer(column.dataFile);
    }

    public int getTotalRead() {
        return totalRead;
    }

    public long getTotalTime() {
        return ioTime;
    }

    public long getReadLength() {
        return readLength;
    }

    public int getCreated() {
        return GlobalInformation.totalBlockManagerCreated;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public long getColumnBlockTime() {
        return colBlockTime;
    }

    public long getColumnStartTime() {
        return colStartTime;
    }

    public int getBufferSize() {
        return bufferSize;
    }
}

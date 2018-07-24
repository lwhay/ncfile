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
    public static final int DEFAULT_SCALE = 16;
    public static final int MAX_FETCH_SIZE = 256 * 1024;
    private final int blockSize;
    private final int cacheScale;
    private final int columnNumber;
    private final byte[][] columnBuffer;
    private final int bufferSize;
    //private Input in; // Need to be encapsulated.
    private int totalRead = 0;

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
            columnBuffer[i] = new byte[bufferSize];
        }
    }

    public int fetch(final Input in, long offset, byte[] b, int start, int len) throws IOException {
        return read(in, offset, b, start, len);
    }

    public int read(final Input in, long offset, byte[] b, int start, int len) throws IOException {
        //System.out.println(offset + "+" + len + "=" + (offset + len));
        int readlen = in.read(offset, b, start, len);
        totalRead++;
        return readlen;
    }

    public void getBlock() {
        //this.in = new InputBuffer(column.dataFile);
    }

    public int getTotalRead() {
        return totalRead;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }
}

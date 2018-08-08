/**
 * 
 */
package columnar;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.trevni.Input;

import exceptions.NeciRuntimeException;
import io.AsyncIOWorker;
import io.BlockInputBuffer;
import io.BlockInputBufferQueue;
import io.PositionalBlock;

/**
 * @author Michael
 *
 */
public class BlockManager {
    public final boolean AIO_OPEN = false;
    public static final boolean TRACE_IO = true;
    public static final int QUEUE_LENGTH_LOW_THRESHOLD = 16;
    public static final int QUEUE_LENGTH_HIGH_THRESHOLD = 32;
    public static final int MAX_FETCH_SIZE = 256 * 1024;
    private final int blockSize;
    private final int cacheScale;
    private final int columnNumber;
    private final int bufferSize;
    private BlockInputBufferQueue[] bufferQueues;
    //private Input in; // Need to be encapsulated.
    private int totalRead = 0;
    private long ioTime = 0;
    private long compressionTime = 0;
    private long readLength = 0;
    public long colBlockTime = 0;
    public long colStartTime = 0;
    public int headerIOs = 0;
    private int totalBlockCreation = 0;
    private ExecutorService ioService;
    private AsyncIOWorker ioWorker;
    private Short ioPending = 0;

    public BlockManager(int bs, int cs) {
        this(bs, cs, 0);
    }

    public BlockManager(int bs) {
        this(bs, QUEUE_LENGTH_HIGH_THRESHOLD);
    }

    public BlockManager(int bs, int cs, int col) {
        this.blockSize = bs * 1024;
        this.cacheScale = cs;
        this.columnNumber = col;
        this.bufferSize = (blockSize * cacheScale > MAX_FETCH_SIZE) ? MAX_FETCH_SIZE : blockSize * cacheScale;
        if (AIO_OPEN) {
            this.bufferQueues = new BlockInputBufferQueue[columnNumber];
            for (int i = 0; i < columnNumber; i++) {
                bufferQueues[i] = new BlockInputBufferQueue(MAX_FETCH_SIZE);
            }
        }
        GlobalInformation.totalBlockManagerCreated++;
    }

    public void openAio(BlockColumnValues[] columnValues) throws InterruptedException, IOException {
        if (AIO_OPEN) {
            ioService = Executors.newCachedThreadPool();
            ioWorker = new AsyncIOWorker(columnValues, bufferQueues, ioPending);
            ioService.execute(ioWorker);
        }
    }

    public void closeAio() throws InterruptedException {
        if (AIO_OPEN) {
            ioWorker.terminate();
            ioService.shutdown();
        }
    }

    public void trigger(boolean[] intends) {
        if (AIO_OPEN) {
            ioWorker.trigger(intends);
            synchronized (ioPending) {
                ioPending.notify();
            }
        }
    }

    public BlockInputBufferQueue[] getBufferQueues() {
        return this.bufferQueues;
    }

    public PositionalBlock<Integer, BlockInputBuffer> fetch(int cidx, int block) throws InterruptedException {
        PositionalBlock<Integer, BlockInputBuffer> intendedBlock;
        do {
            intendedBlock = bufferQueues[cidx].take();
        } while (intendedBlock.getKey() < block);

        if (intendedBlock.getKey() != block) {
            throw new NeciRuntimeException("Loose blocks: " + block + " at: " + cidx);
        }
        return intendedBlock;
    }

    public void blockAdd() {
        this.totalBlockCreation++;
    }

    public int getBlockCreation() {
        return this.totalBlockCreation;
    }

    public void compressionTimeAdd(long tick) {
        this.compressionTime += tick;
    }

    public long getCompressionTime() {
        return this.compressionTime;
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
        return colBlockTime / 1000000000;
    }

    public long getColumnStartTime() {
        return colStartTime / 1000000000;
    }

    public int getBufferSize() {
        return bufferSize;
    }
}

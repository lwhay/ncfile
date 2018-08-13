/**
 * 
 */
package columnar;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.trevni.Input;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import exceptions.NeciRuntimeException;
import io.AsyncIOWorker;
import io.BlockInputBuffer;
import misc.BlockInputBufferQueue;
import misc.PositionalBlock;

/**
 * @author Michael
 *
 */
public class BlockManager {
    private final int blockSize;
    private final int cacheScale;
    private final int columnNumber;
    private final int bufferSize;

    public static final String dbconf = "./dbconf.json";
    public static final String swapmm = "./swap.mm";
    public static boolean AIO_OPEN = true;
    public static boolean TRACE_IO = true;
    public static boolean FILE_LOCK = false;
    public static boolean SKIPPING_MODE = true;
    public static int QUEUE_SLOT_DEFAULT_SIZE = 32;
    public static int QUEUE_LENGTH_LOW_THRESHOLD = 192;
    public static int QUEUE_LENGTH_HIGH_THRESHOLD = 256;
    public static int MAX_FETCH_SIZE = 256 * 1024;

    public long colBlockTime = 0;
    public long colStartTime = 0;
    public int headerIOs = 0;

    private BlockInputBufferQueue[] bufferQueues;
    //private Input in; // Need to be encapsulated.
    private int totalRead = 0;
    private long ioTime = 0;
    private long compressionTime = 0;
    private long readLength = 0;
    private long aioTime = 0;
    private long aioFetchTime = 0;
    private int totalBlockCreation = 0;
    private final PositionalBlock<Integer, BlockInputBuffer>[][] currentBlocks;
    private final int cursors[];
    private ExecutorService ioService;
    private AsyncIOWorker ioWorker;
    private Short ioPending = 0;
    private boolean isFetching = false;

    public BlockManager(int bs, int cs) {
        this(bs, cs, 0);
    }

    public BlockManager(int bs) {
        this(bs, QUEUE_LENGTH_HIGH_THRESHOLD);
    }

    @SuppressWarnings("unchecked")
    public BlockManager(int bs, int cs, int col) {
        this.blockSize = bs * 1024;
        this.cacheScale = cs;
        this.columnNumber = col;
        this.bufferSize = (blockSize * cacheScale > MAX_FETCH_SIZE) ? MAX_FETCH_SIZE : blockSize * cacheScale;
        this.currentBlocks = new PositionalBlock[columnNumber][];
        this.cursors = new int[columnNumber];
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode conf = mapper.readTree(new File(dbconf));
            AIO_OPEN = conf.path("AIO_OPEN").asBoolean();
            TRACE_IO = conf.path("TRACE_IO").asBoolean();
            FILE_LOCK = conf.path("FILE_LOCK").asBoolean();
            SKIPPING_MODE = conf.path("SKIPPING_MODE").asBoolean();
            QUEUE_SLOT_DEFAULT_SIZE = conf.path("QUEUE_SLOT_DEFAULT_SIZE").asInt();
            QUEUE_LENGTH_LOW_THRESHOLD = conf.path("QUEUE_LENGTH_LOW_THRESHOLD").asInt();
            QUEUE_LENGTH_HIGH_THRESHOLD = conf.path("QUEUE_LENGTH_HIGH_THRESHOLD").asInt();
            MAX_FETCH_SIZE = conf.path("MAX_FETCH_SIZE").asInt() * 1024;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Arrays.fill(cursors, BlockManager.QUEUE_SLOT_DEFAULT_SIZE);
        if (AIO_OPEN) {
            this.bufferQueues = new BlockInputBufferQueue[columnNumber];
            for (int i = 0; i < columnNumber; i++) {
                bufferQueues[i] = new BlockInputBufferQueue(QUEUE_LENGTH_HIGH_THRESHOLD);
            }
        }
        GlobalInformation.totalBlockManagerCreated++;
    }

    @SuppressWarnings("rawtypes")
    public void openAio(BlockColumnValues[] columnValues) throws InterruptedException, IOException {
        if (AIO_OPEN) {
            ioService = Executors.newCachedThreadPool();
            ioWorker = new AsyncIOWorker(columnValues, bufferQueues, ioPending);
            ioService.execute(ioWorker);
        }
    }

    public void diableAio() {
        if (AIO_OPEN) {
            AIO_OPEN = false;
        }
    }

    public void closeAio() throws InterruptedException {
        if (AIO_OPEN) {
            ioWorker.terminate();
            ioService.shutdown();
        }
    }

    public void setSkip(boolean openSkipping) {
        if (AIO_OPEN) {
            ioWorker.setSkip(openSkipping);
        }
    }

    public boolean getSkip() {
        if (AIO_OPEN) {
            return ioWorker.getSkip();
        }
        return false;
    }

    public void trigger(boolean[] intends, BitSet[] valids) {
        if (AIO_OPEN) {
            ioWorker.trigger(intends, valids);
            isFetching = true;
        }
    }

    public boolean isFetchingStage() {
        return isFetching;
    }

    public void invalid(int cidx) {
        if (AIO_OPEN) {
            ioWorker.invalid(cidx);
            currentBlocks[cidx] = null;
            cursors[cidx] = BlockManager.QUEUE_SLOT_DEFAULT_SIZE;
        }
    }

    public boolean trigger(int cidx, BitSet valid) {
        if (AIO_OPEN) {
            if (ioWorker.trigger(cidx, valid)) {
                currentBlocks[cidx] = null;
                cursors[cidx] = BlockManager.QUEUE_SLOT_DEFAULT_SIZE;
                return true;
            }
        }
        return false;
    }

    public BlockInputBufferQueue[] getBufferQueues() {
        return this.bufferQueues;
    }

    public PositionalBlock<Integer, BlockInputBuffer> fetch(int cidx, int block) throws InterruptedException {
        int found = -1;
        /*if (isFetching)
            System.out.println("\t>Dequeue " + block + " " + ioWorker.getColumnValue(cidx).getName() + " "
                    + ioWorker.getColumnValue(cidx).getBlockCount());*/
        while (found < 0) {
            if (cursors[cidx] < BlockManager.QUEUE_SLOT_DEFAULT_SIZE) {
                while (cursors[cidx] < currentBlocks[cidx].length) {
                    if (currentBlocks[cidx][cursors[cidx]].getKey() == block) {
                        found = cursors[cidx]++;
                        break;
                    }
                    ++cursors[cidx];
                    if (cursors[cidx] > block + 1) {
                        /*System.out.println(ioWorker.getValid(cidx).get(0, 10260));
                        for (int t = 0; t < 10; t++) {
                            String hint = "";
                            for (int i = ioWorker.getColumnValue(cidx).getColumnDescriptor().firstRows[cursors[cidx]
                                    + t]; i < ioWorker.getColumnValue(cidx).getColumnDescriptor()
                                            .lastRow(cursors[cidx] + t); i++) {
                                if (ioWorker.getValid(cidx).get(i)) {
                                    hint += "1";
                                } else {
                                    hint += "0";
                                }
                            }
                            System.out.println("Exception on " + cidx + " " + block + "\n" + hint);
                        }*/
                        throw new NeciRuntimeException("Lose block on Sender: " + cidx + " " + block);
                    }
                }
            }
            if (found < 0) {
                long begin = System.nanoTime();
                currentBlocks[cidx] = bufferQueues[cidx].take();
                aioFetchTime += (System.nanoTime() - begin);
                cursors[cidx] = 0;
            }
        }
        /*if (isFetching)
            System.out.println("\t<Dequeue " + block + " " + ioWorker.getColumnValue(cidx).getName() + " "
                    + ioWorker.getColumnValue(cidx).getBlockCount());*/
        return currentBlocks[cidx][found];
    }

    public void blockAdd() {
        this.totalBlockCreation++;
    }

    public int getBlockCreation() {
        return this.totalBlockCreation;
    }

    public void aioTimeAdd(long tick) {
        this.aioTime += tick;
    }

    public long getAioTime() {
        return this.aioTime;
    }

    public long getAioFetchTime() {
        return this.aioFetchTime;
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

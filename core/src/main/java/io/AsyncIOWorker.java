/**
 * 
 */
package io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import codec.CodecFactory;
import columnar.BlockColumnValues;
import columnar.BlockManager;
import columnar.ColumnDescriptor;
import exceptions.NeciRuntimeException;
import misc.BlockInputBufferQueue;
import misc.PositionalBlock;
import misc.ValueType;

/**
 * @author Michael
 *
 */
public class AsyncIOWorker implements Runnable {
    @SuppressWarnings("rawtypes")
    private final BlockColumnValues[] columnValues;
    private final BlockInputBufferQueue[] queues;
    private final Short ioPending;
    private final int[] blocks;
    private final int[] rows;
    @SuppressWarnings("rawtypes")
    private final ColumnDescriptor[] columns;
    private final boolean[] isUnion;
    private final int[] unionBits;
    private final ValueType[][] unionArray;
    private final InputBuffer[] inBuffers;
    private boolean terminate = false;
    private boolean[] intended;
    private BitSet[] valids;
    private boolean isReady = true;
    private final Short ioReady;
    private FileChannel mmc = null;
    private FileLock serialReadLock = null;
    private boolean skippingMode = false;
    private boolean isFetching = false;
    private int period = 10;
    private long totalPayload = 0;
    private long processedPayload = 0;
    private long currentPriority = 5;
    private int intendingColumns = 0;
    private long lastProcessingPeriod = 0;
    private int CompressionThreads = 0;

    private BlockInputBuffer[][] dcpCaches;
    private Integer[] idling;
    private Thread[] dcpWorkers;
    private DecompressionWorker[] dcpRunners;

    @SuppressWarnings({ "static-access", "resource", "rawtypes" })
    public AsyncIOWorker(BlockColumnValues[] columnValues, BlockInputBufferQueue[] queues, Short ioPending)
            throws IOException {
        this.columnValues = columnValues;
        this.blocks = new int[columnValues.length];
        this.ioPending = ioPending;
        this.rows = new int[columnValues.length];
        this.queues = queues;
        this.inBuffers = new InputBuffer[columnValues.length];
        this.isUnion = new boolean[columnValues.length];
        this.unionBits = new int[columnValues.length];
        this.unionArray = new ValueType[columnValues.length][];
        this.columns = new ColumnDescriptor[columnValues.length];
        this.ioReady = 0;
        intended = new boolean[columnValues.length];
        valids = new BitSet[columnValues.length];
        boolean useCompression = false;
        for (int i = 0; i < columnValues.length; i++) {
            if (columnValues[i] != null) {
                this.columns[i] = columnValues[i].getColumnDescriptor();
                inBuffers[i] = new InputBuffer(columns[i].getBlockManager(), columns[i].getDataFile());
                if (columnValues[i].getType().equals(ValueType.UNION)) {
                    isUnion[i] = true;
                    unionBits[i] = columns[i].metaData.getUnionBits();
                    unionArray[i] = columns[i].metaData.getUnionArray();
                } else {
                    isUnion[i] = false;
                }
                if (mmc == null && columns[i].getBlockManager().FILE_LOCK) {
                    try {
                        mmc = new RandomAccessFile(columns[i].getBlockManager().swapmm, "rw").getChannel();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                }
                if (!skippingMode && columns[i].getBlockManager().SKIPPING_MODE) {
                    skippingMode = true;
                }
                if (!columns[i].getCodecName().equals("null")) {
                    useCompression = true;
                }
            }
        }
        if (skippingMode && useCompression && BlockManager.COMPRESSION_THREADS > 0) {
            CompressionThreads = BlockManager.COMPRESSION_THREADS;
            idling = new Integer[CompressionThreads];
            dcpCaches = new BlockInputBuffer[CompressionThreads][];
            dcpWorkers = new Thread[CompressionThreads];
            dcpRunners = new DecompressionWorker[CompressionThreads];
            /*System.out.println("Number of compression threads: " + CompressionThreads);*/
            for (int i = 0; i < CompressionThreads; i++) {
                idling[i] = i;
                dcpRunners[i] = new DecompressionWorker(idling[i]);
                dcpWorkers[i] = new Thread(dcpRunners[i]);
                dcpWorkers[i].start();
                dcpCaches[i] = new BlockInputBuffer[BlockManager.QUEUE_SLOT_DEFAULT_SIZE / CompressionThreads];
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public BlockColumnValues getColumnValue(int cidx) {
        return columnValues[cidx];
    }

    public BitSet getValid(int cidx) {
        return valids[cidx];
    }

    public void setSkip(boolean skippingMode) {
        this.skippingMode = skippingMode;
    }

    public boolean getSkip() {
        return skippingMode;
    }

    public void trigger(boolean[] intends, BitSet[] valids) {
        /*System.out.println("Fetching");*/
        while (!isReady) {
            synchronized (ioReady) {
                try {
                    ioReady.wait();
                } catch (InterruptedException e) {
                    throw new NeciRuntimeException("Cannot trigger when fetching");
                }
            }
        }
        isFetching = true;
        isReady = false;
        this.intended = intends;
        this.valids = valids;
        intendingColumns++;
        for (int i = 0; i < intended.length; i++) {
            if (intended[i]) {
                blocks[i] = 1;
                rows[i] = columns[i].lastRow(0) + 1;
                if (dcpRunners != null) {
                    for (DecompressionWorker runner : dcpRunners) {
                        if (runner.getCodec() == null || !runner.getCodec().getClass().getName()
                                .equals(columnValues[i].getCodec().getClass().getName())) {
                            runner.setCodec(new CodecFactory(columns[i].metaData));
                        }
                    }
                }
            }
        }
        synchronized (ioPending) {
            ioPending.notify();
        }
    }

    public void invalid(int cidx) {
        intended[cidx] = false;
        blocks[cidx] = 0;
        rows[cidx] = 0;
        synchronized (ioPending) {
            ioPending.notify();
        }
        isReady = false;
    }

    public boolean isValid(int cidx) {
        return intended[cidx];
    }

    public boolean trigger(int cidx, BitSet valid) {
        /*System.out.println("Trigger " + columns[cidx].metaData.getName());*/
        while (intended[cidx]) {
            try {
                System.out.println("@");
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (columns[cidx].getBlockManager().isFetchingStage()) {
            if (columnValues[cidx].isArray()) {
                return false;
            } else {
                throw new NeciRuntimeException("Duplicated intended " + cidx + columnValues[cidx].getName());
            }
        }

        if (intended[cidx]) {
            throw new NeciRuntimeException("Duplicated intended " + cidx + columnValues[cidx].getName());
        }

        while (!isReady) {
            synchronized (ioReady) {
                try {
                    ioReady.wait();
                } catch (InterruptedException e) {
                    throw new NeciRuntimeException("Cannot trigger: " + cidx);
                }
            }
        }
        isReady = false;

        blocks[cidx] = 1;
        rows[cidx] = columns[cidx].lastRow(0) + 1;
        intended[cidx] = true;
        valids[cidx] = valid;
        intendingColumns++;

        if (dcpRunners != null) {
            for (DecompressionWorker runner : dcpRunners) {
                if (runner.getCodec() == null || !runner.getCodec().getClass().getName()
                        .equals(columnValues[cidx].getCodec().getClass().getName())) {
                    runner.setCodec(new CodecFactory(columns[cidx].metaData));
                }
            }
        }
        synchronized (ioPending) {
            ioPending.notify();
        }
        return true;
    }

    public void terminate() {
        /*System.out.println("Terminate");*/
        terminate = true;
        if (mmc != null) {
            try {
                mmc.close();
            } catch (IOException e) {
                throw new NeciRuntimeException("Cannot close lockfile");
            }
        }
        while (!isReady) {
            synchronized (ioReady) {
                try {
                    ioReady.wait();
                } catch (InterruptedException e) {
                    throw new NeciRuntimeException("Cannot terminate");
                }
            }
        }
        synchronized (ioPending) {
            ioPending.notify();
        }
        isReady = false;
        if (dcpWorkers != null) {
            int runnerId = 0;
            for (Thread worker : dcpWorkers) {
                synchronized (idling[runnerId]) {
                    idling[runnerId].notify();
                }
                worker.interrupt();
                runnerId++;
            }
        }
        System.exit(0);
    }

    private void lock() throws IOException {
        if (mmc != null) {
            serialReadLock = mmc.lock();
        }
    }

    private void release() throws IOException {
        if (serialReadLock != null) {
            serialReadLock.release();
        }
    }

    @SuppressWarnings("unchecked")
    private boolean sequentialRead() throws IOException, InterruptedException {
        boolean idle = true;
        boolean completed = true;
        for (int i = 0; i < queues.length; i++) {
            if (!intended[i]) {
                continue;
            }
            completed = false;
            if (queues[i].size() < BlockManager.QUEUE_LENGTH_LOW_THRESHOLD) {
                idle = false;
                int total = Math.min(columnValues[i].getBlockCount() - blocks[i],
                        (BlockManager.QUEUE_LENGTH_HIGH_THRESHOLD - queues[i].size())
                                * BlockManager.QUEUE_SLOT_DEFAULT_SIZE);
                BlockInputBuffer[] bufs = startBlock(i, total);
                int regular = 0;
                for (int j = 0; j < total / BlockManager.QUEUE_SLOT_DEFAULT_SIZE; j++) {
                    PositionalBlock<Integer, BlockInputBuffer>[] list =
                            new PositionalBlock[BlockManager.QUEUE_SLOT_DEFAULT_SIZE];
                    for (int k = 0; k < BlockManager.QUEUE_SLOT_DEFAULT_SIZE; k++) {
                        list[k] = new PositionalBlock<Integer, BlockInputBuffer>(blocks[i] + regular, bufs[regular]);
                        regular++;
                    }
                    queues[i].put(list);
                }
                if (regular < total) {
                    PositionalBlock<Integer, BlockInputBuffer>[] list = new PositionalBlock[total - regular];
                    for (int k = 0; regular < total; k++) {
                        list[k] = new PositionalBlock<Integer, BlockInputBuffer>(blocks[i] + regular, bufs[regular]);
                        regular++;
                    }
                    queues[i].put(list);
                }
                this.blocks[i] += total;
                if (this.blocks[i] == columnValues[i].getBlockCount()) {
                    intended[i] = false;
                }
            }
        }
        if (completed) {
            isReady = true;
            synchronized (ioReady) {
                ioReady.notify();
            }
            while (isReady) {
                synchronized (ioPending) {
                    ioPending.wait();
                }
            }
        }
        return idle;
    }

    private BlockInputBuffer[] startBlock(int cidx, int num) throws IOException {
        byte[][] raws = new byte[num][];
        int[] ends = new int[num];
        /*String hint = "";*/
        lock();
        for (int i = 0; i < num; i++) {
            int block = this.blocks[cidx] + i;
            this.rows[cidx] = columns[cidx].firstRows[block];
            /*int m = valids[cidx].nextSetBit(this.rows[cidx]);
            if (m >= 0 && m <= columns[cidx].lastRow(block)) {
                hint += "1";
            } else {
                hint += "0";
            }*/
            /*if (valids[cidx].size() != columns[cidx].lastRow()) {
                throw new NeciRuntimeException(valids[cidx].size() + ":" + columns[cidx].lastRow());
            }*/
            inBuffers[cidx].seek(columns[cidx].blockStarts[block]);
            ends[i] = columns[cidx].blocks[block].getCompressedSize();
            raws[i] = new byte[ends[i] + columnValues[cidx].getChecksum().size()];
            inBuffers[cidx].readFully(raws[i]);
        }
        release();
        /*System.out.println(columnValues[cidx].getName() + " from " + this.blocks[cidx] + " to "
                + (this.blocks[cidx] + num) + " hint " + hint);*/
        BlockInputBuffer[] values = new BlockInputBuffer[num];
        long beginCompression = System.nanoTime();
        for (int i = 0; i < num; i++) {
            values[i] = decompression(cidx, this.blocks[cidx] + i, raws[i], ends[i]);
        }
        columns[cidx].getBlockManager().compressionTimeAdd(System.nanoTime() - beginCompression);
        return values;
    }

    private boolean skippingRead() throws IOException, InterruptedException {
        boolean idle = true;
        boolean completed = true;
        totalPayload = 0;
        processedPayload = 0;
        for (int i = 0; i < queues.length; i++) {
            if (!intended[i]) {
                continue;
            }
            totalPayload += columnValues[i].getBlockCount();
            processedPayload += this.blocks[i];
            completed = false;
            if (queues[i].size() < BlockManager.QUEUE_LENGTH_LOW_THRESHOLD) {
                idle = false;
                int total = Math.min(columnValues[i].getBlockCount() - blocks[i],
                        (BlockManager.QUEUE_LENGTH_HIGH_THRESHOLD - queues[i].size())
                                * BlockManager.QUEUE_SLOT_DEFAULT_SIZE);
                List<PositionalBlock<Integer, BlockInputBuffer>[]> slots = skippingBlock(i, total, valids[i]);
                /*System.out.println("!!!!c" + i + " n" + total + "v" + slots.get(0)[0].getValue());*/
                for (PositionalBlock<Integer, BlockInputBuffer>[] slot : slots) {
                    queues[i].put(slot);
                }
                if (this.blocks[i] == columnValues[i].getBlockCount()) {
                    intended[i] = false;
                    /*System.out.println("Complete " + columns[i].metaData.getName());*/
                }
            }
        }
        if (completed) {
            isReady = true;
            synchronized (ioReady) {
                ioReady.notify();
            }
            while (isReady) {
                synchronized (ioPending) {
                    ioPending.wait();
                }
            }
        }
        return idle;
    }

    @SuppressWarnings("unchecked")
    private List<PositionalBlock<Integer, BlockInputBuffer>[]> skippingBlock(int cidx, int num, BitSet valid)
            throws IOException {
        // Prepare.
        int packed = 0;
        int cursor = 0;
        int[] bids = new int[num];
        long[] pos = new long[num];
        int[] ends = new int[num];
        byte[][] raws = new byte[num][];
        /*String hint = "";
        System.out.println(">Prepare " + cidx + " " + columns[cidx].metaData.getName() + " " + blocks[cidx] + " "
                + columns[cidx].blockCount() + " " + num);*/
        while (packed < num && blocks[cidx] + cursor < columns[cidx].blockCount()) {
            int bid = blocks[cidx] + cursor;
            rows[cidx] = columns[cidx].firstRows[bid];
            /*if (cidx == 24 && bid == 0) {
                System.out.println(valids[cidx].get(0, 10260));
                for (int t = 0; t < 10; t++) {
                    String bits = "";
                    for (int i = columns[cidx].firstRows[bid + t]; i < columns[cidx].lastRow(bid + t); i++) {
                        if (valids[cidx].get(i)) {
                            bits += "1";
                        } else {
                            bits += "0";
                        }
                    }
                    System.out.println(bits);
                }
            }*/
            /*int m = valids[cidx].nextSetBit(this.rows[cidx]);
            if (m >= 0 && m <= columns[cidx].lastRow(bid)) {
                hint += "1";
            } else {
                hint += "0";
            }*/
            int nextHit = valids[cidx].nextSetBit(rows[cidx]);
            if ((!isFetching && columnValues[cidx].isArray()) /*|| bid == 0*/
                    || nextHit >= 0 && nextHit <= columns[cidx].lastRow(bid)) {
                bids[packed] = bid;
                pos[packed] = columns[cidx].blockStarts[bid];
                ends[packed] = columns[cidx].blocks[bid].getCompressedSize();
                raws[packed] = new byte[ends[packed] + columnValues[cidx].getChecksum().size()];
                packed++;
            } else {
                if (nextHit > columns[cidx].lastRow(bid)) {
                    int k = columns[cidx].findBlock(nextHit);
                    bids[packed] = k;
                    pos[packed] = columns[cidx].blockStarts[k];
                    ends[packed] = columns[cidx].blocks[k].getCompressedSize();
                    raws[packed] = new byte[ends[packed] + columnValues[cidx].getChecksum().size()];
                    packed++;
                    cursor += (k - bid);
                    rows[cidx] = columns[cidx].firstRows[blocks[cidx] + cursor];
                    /*for (int k = bid + 1; k < columns[cidx].blockCount(); k++) {
                        if (nextHit <= columns[cidx].lastRow(k)) {
                            bids[packed] = k;
                            pos[packed] = columns[cidx].blockStarts[k];
                            ends[packed] = columns[cidx].blocks[k].getCompressedSize();
                            raws[packed] = new byte[ends[packed] + columnValues[cidx].getChecksum().size()];
                            packed++;
                            cursor += (k - bid);
                            rows[cidx] = columns[cidx].firstRows[blocks[cidx] + cursor];
                            System.out.println(cidx + " c" + cursor + " k" + k + " bid" + bid + " next" + nextHit
                                    + " all" + columns[cidx].lastRow(k) + " bc" + columns[cidx].blockCount());
                            break;
                        }
                    }*/
                }
                if (nextHit < 0 || nextHit >= columns[cidx].lastRow()) {
                    cursor = columns[cidx].blockCount() - blocks[cidx] - 1;
                    //System.out.println("Hello sb." + nextHit);
                }
            }
            cursor++;
        }
        /*System.out.println("<Prepare " + cidx + " " + columns[cidx].metaData.getName() + " "
                + columns[cidx].blockCount() + " " + packed + " " + cursor + "\n" + hint);*/
        blocks[cidx] += cursor;

        // Read.
        lock();
        for (int i = 0; i < packed; i++) {
            inBuffers[cidx].seek(pos[i]);
            inBuffers[cidx].readFully(raws[i]);
        }
        release();

        // Decompression.
        long beginCompression = System.nanoTime();
        List<PositionalBlock<Integer, BlockInputBuffer>[]> packedlist =
                new ArrayList<PositionalBlock<Integer, BlockInputBuffer>[]>();
        if (dcpWorkers == null) {
            int regular = 0;
            for (int i = 0; i < packed / BlockManager.QUEUE_SLOT_DEFAULT_SIZE; i++) {
                PositionalBlock<Integer, BlockInputBuffer>[] pbs =
                        new PositionalBlock[BlockManager.QUEUE_SLOT_DEFAULT_SIZE];
                for (int k = 0; k < BlockManager.QUEUE_SLOT_DEFAULT_SIZE; k++) {
                    BlockInputBuffer value = decompression(cidx, bids[regular], raws[regular], ends[regular]);
                    pbs[k] = new PositionalBlock<Integer, BlockInputBuffer>(bids[regular], value);
                    /*System.out.println("\tEnqueue " + bids[regular] + " " + columns[cidx].metaData.getName() + " "
                        + columns[cidx].blockCount());*/
                    /*System.out.println("\t=" + bids[regular]);*/
                    regular++;
                }
                packedlist.add(pbs);
            }

            if (regular < packed) {
                PositionalBlock<Integer, BlockInputBuffer>[] pbs = new PositionalBlock[packed - regular];
                int rest = packed - regular;
                /*System.out.println("Enqueue " + bids[regular] + " " + columns[cidx].metaData.getName() + " "
                    + columns[cidx].blockCount() + " " + rest + " " + regular + " " + packed);*/
                for (int k = 0; k < rest; k++) {
                    BlockInputBuffer value = decompression(cidx, bids[regular], raws[regular], ends[regular]);
                    pbs[k] = new PositionalBlock<Integer, BlockInputBuffer>(bids[regular], value);
                    /*System.out.println("\t+Enqueue " + bids[regular] + " " + columns[cidx].metaData.getName() + " "
                        + columns[cidx].blockCount());*/
                    /*System.out.println("\t=" + bids[regular]);*/
                    regular++;
                }
                packedlist.add(pbs);
            }
        } else {
            int regular = 0;
            for (int i = 0; i < packed / BlockManager.QUEUE_SLOT_DEFAULT_SIZE; i++) {
                PositionalBlock<Integer, BlockInputBuffer>[] pbs =
                        new PositionalBlock[BlockManager.QUEUE_SLOT_DEFAULT_SIZE];
                int taskGran = BlockManager.QUEUE_SLOT_DEFAULT_SIZE / CompressionThreads;
                List<DecompressionTask> tasks = new ArrayList<DecompressionTask>();
                int tid = 0;
                for (int k = 0; k < BlockManager.QUEUE_SLOT_DEFAULT_SIZE; k++) {
                    tasks.add(new DecompressionTask(cidx, bids[regular], raws[regular], ends[regular]));
                    if ((k + 1) % taskGran == 0) {
                        dcpRunners[tid].trigger(tasks);
                        while (!dcpRunners[tid].running) {
                            synchronized (idling[tid]) {
                                idling[tid].notify();
                            }
                        }
                        tasks = new ArrayList<DecompressionTask>();
                        tid++;
                    }
                    regular++;
                }

                for (int pid = 0; pid < CompressionThreads; pid++) {
                    synchronized (idling[pid]) {
                        try {
                            idling[pid].wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        dcpRunners[pid].running = false;
                    }
                    for (int k = 0; k < taskGran; k++) {
                        pbs[pid * taskGran + k] = new PositionalBlock<Integer, BlockInputBuffer>(
                                dcpRunners[pid].getTasks().get(k).bidx, dcpCaches[pid][k]);
                    }
                }
                packedlist.add(pbs);
            }

            if (regular < packed) {
                PositionalBlock<Integer, BlockInputBuffer>[] pbs = new PositionalBlock[packed - regular];
                int rest = packed - regular;
                int taskGran = rest / CompressionThreads + 1;
                List<DecompressionTask> tasks = new ArrayList<DecompressionTask>();
                int tid = 0;
                for (int k = 0; k < rest; k++) {
                    tasks.add(new DecompressionTask(cidx, bids[regular], raws[regular], ends[regular]));
                    if ((k + 1) % taskGran == 0 || k == rest - 1) {
                        dcpRunners[tid].trigger(tasks);
                        while (!dcpRunners[tid].running) {
                            synchronized (idling[tid]) {
                                idling[tid].notify();
                            }
                        }
                        tasks = new ArrayList<DecompressionTask>();
                        tid++;
                    }
                    regular++;
                }
                int producedBlock = 0;
                for (int pid = 0; pid < tid; pid++) {
                    synchronized (idling[pid]) {
                        try {
                            idling[pid].wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        dcpRunners[pid].running = false;
                    }
                    for (int k = 0; k < dcpRunners[pid].getTasks().size(); k++) {
                        pbs[producedBlock] = new PositionalBlock<Integer, BlockInputBuffer>(
                                dcpRunners[pid].getTasks().get(k).bidx, dcpCaches[pid][k]);
                        producedBlock++;
                    }
                }
                packedlist.add(pbs);
            }
        }

        columns[cidx].getBlockManager().compressionTimeAdd(System.nanoTime() - beginCompression);
        return packedlist;
    }

    private class DecompressionTask {
        public int cidx;
        public int bidx;
        public byte[] raw;
        public int len;

        public DecompressionTask(int cidx, int bidx, byte[] raw, int len) {
            this.cidx = cidx;
            this.bidx = bidx;
            this.raw = raw;
            this.len = len;
        }
    }

    private class DecompressionWorker implements Runnable {
        public boolean running = false;
        private final Integer tid;
        private List<DecompressionTask> tasks;
        private CodecFactory codec = null;

        public DecompressionWorker(Integer tid) {
            this.tid = tid;
        }

        public void trigger(List<DecompressionTask> tasks) {
            this.tasks = tasks;
        }

        public List<DecompressionTask> getTasks() {
            return tasks;
        }

        public void setCodec(CodecFactory codec) {
            this.codec = codec;
        }

        public CodecFactory getCodec() {
            return codec;
        }

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                if (terminate) {
                    System.exit(0);
                }
                synchronized (tid) {
                    try {
                        tid.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    running = true;
                }
                try {
                    int taskId = 0;
                    if (tasks != null) { // When terminate command is triggered by close, tasks equals null.
                        for (DecompressionTask task : tasks) {
                            dcpCaches[tid][taskId] = aioDecompression(codec, task.cidx, task.bidx, task.raw, task.len);
                            //dcpCaches[tid][taskId] = decompression(task.cidx, task.bidx, task.raw, task.len);
                            taskId++;
                        }
                        while (running) {
                            synchronized (tid) {
                                tid.notify();
                            }
                        }
                    } else {
                        try {
                            Thread.sleep(BlockManager.CUTOFF_SLEEP_PERIOD);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private BlockInputBuffer aioDecompression(CodecFactory codec, int cidx, int bidx, byte[] raw, int len)
            throws IOException {
        BlockInputBuffer value;
        if (isUnion[cidx]) {
            if (columns[cidx].getCodecName().equals("null")) {
                value = new UnionInputBuffer(ByteBuffer.wrap(raw, 0, len), columns[cidx].blocks[bidx].getRowCount(),
                        unionBits[cidx], unionArray[cidx]);
            } else {
                ByteBuffer data = null;
                if (columns[cidx].blocks[bidx].getUncompressedSize() >= columns[cidx].blocks[bidx]
                        .getCompressedSize()) {
                    data = ByteBuffer.allocate(columns[cidx].blocks[bidx].getUncompressedSize());
                } else {
                    data = ByteBuffer.allocate(columns[cidx].blocks[bidx].getCompressedSize());
                }
                ByteBuffer buf3 =
                        codec.decompress(ByteBuffer.wrap(raw, 0, columns[cidx].blocks[bidx].getLengthUnion()));
                int pos0 = 0;
                int len0 = buf3.limit();
                System.arraycopy(buf3.array(), 0, data.array(), pos0, len0);
                ByteBuffer buf1 = codec.decompress(ByteBuffer.wrap(raw, columns[cidx].blocks[bidx].getLengthUnion(),
                        columns[cidx].blocks[bidx].getLengthOffset()));
                int pos1 = buf3.remaining();
                int len1 = buf1.remaining();
                System.arraycopy(buf1.array(), buf1.position(), data.array(), pos1, len1);
                int pos2 = -1;
                int len2 = -1;
                if (columns[cidx].blocks[bidx].getLengthPayload() != 0) {
                    ByteBuffer buf2 = codec.decompress(ByteBuffer.wrap(raw,
                            columns[cidx].blocks[bidx].getLengthUnion() + columns[cidx].blocks[bidx].getLengthOffset(),
                            columns[cidx].blocks[bidx].getLengthPayload()));
                    pos2 = buf3.remaining() + buf1.remaining();
                    len2 = buf2.remaining();
                    System.arraycopy(buf2.array(), buf2.position(), data.array(), pos2, len2);
                }
                int total = len0 + len1 + len2;
                ByteBuffer rewind = ByteBuffer.allocate(total);
                System.arraycopy(data.array(), 0, rewind.array(), 0, total);
                value = new UnionInputBuffer(rewind, columns[cidx].blocks[bidx].getRowCount(), unionBits[cidx],
                        unionArray[cidx]);
            }
        } else {
            if (columns[cidx].getCodecName().equals("null")) {
                value = new BlockInputBuffer(ByteBuffer.wrap(raw, 0, len), columns[cidx].blocks[bidx].getRowCount());
            } else if (columns[cidx].blocks[bidx].getLengthOffset() != 0) {
                ByteBuffer buf1 = codec.decompress(ByteBuffer.wrap(raw, columns[cidx].blocks[bidx].getLengthUnion(),
                        columns[cidx].blocks[bidx].getLengthOffset()));
                ByteBuffer buf2 = codec.decompress(ByteBuffer.wrap(raw,
                        columns[cidx].blocks[bidx].getLengthUnion() + columns[cidx].blocks[bidx].getLengthOffset(),
                        columns[cidx].blocks[bidx].getLengthPayload()));
                int total = buf1.remaining() + buf2.remaining();
                ByteBuffer data = ByteBuffer.allocate(total);
                System.arraycopy(buf1.array(), 0, data.array(), 0, buf1.remaining());
                System.arraycopy(buf2.array(), buf2.position(), data.array(), buf1.remaining(), buf2.remaining());
                value = new BlockInputBuffer(data, columns[cidx].blocks[bidx].getRowCount());
            } else {
                ByteBuffer buf2 = codec.decompress(ByteBuffer.wrap(raw,
                        columns[cidx].blocks[bidx].getLengthUnion() + columns[cidx].blocks[bidx].getLengthOffset(),
                        columns[cidx].blocks[bidx].getLengthPayload()));
                int total = buf2.remaining();
                ByteBuffer data = ByteBuffer.allocate(total);
                System.arraycopy(buf2.array(), 0, data.array(), 0, total);
                value = new BlockInputBuffer(/*ByteBuffer.wrap(buf2)*/data, columns[cidx].blocks[bidx].getRowCount());
            }
        }
        /*System.out.println("%%%%c" + cidx + " b" + bidx + " v" + value + " l" + len);*/
        return value;
    }

    private BlockInputBuffer decompression(int cidx, int bidx, byte[] raw, int len) throws IOException {
        BlockInputBuffer value;
        if (isUnion[cidx]) {
            if (columns[cidx].getCodecName().equals("null")) {
                value = new UnionInputBuffer(ByteBuffer.wrap(raw, 0, len), columns[cidx].blocks[bidx].getRowCount(),
                        unionBits[cidx], unionArray[cidx]);
            } else {
                ByteBuffer data = null;
                if (columns[cidx].blocks[bidx].getUncompressedSize() >= columns[cidx].blocks[bidx]
                        .getCompressedSize()) {
                    data = ByteBuffer.allocate(columns[cidx].blocks[bidx].getUncompressedSize());
                } else {
                    data = ByteBuffer.allocate(columns[cidx].blocks[bidx].getCompressedSize());
                }
                ByteBuffer buf3 = columnValues[cidx].getCodec()
                        .decompress(ByteBuffer.wrap(raw, 0, columns[cidx].blocks[bidx].getLengthUnion()));
                int pos0 = 0;
                int len0 = buf3.limit();
                System.arraycopy(buf3.array(), 0, data.array(), pos0, len0);
                ByteBuffer buf1 = columnValues[cidx].getCodec().decompress(ByteBuffer.wrap(raw,
                        columns[cidx].blocks[bidx].getLengthUnion(), columns[cidx].blocks[bidx].getLengthOffset()));
                int pos1 = buf3.remaining();
                int len1 = buf1.remaining();
                System.arraycopy(buf1.array(), buf1.position(), data.array(), pos1, len1);
                int pos2 = -1;
                int len2 = -1;
                if (columns[cidx].blocks[bidx].getLengthPayload() != 0) {
                    ByteBuffer buf2 = columnValues[cidx].getCodec()
                            .decompress(ByteBuffer.wrap(raw,
                                    columns[cidx].blocks[bidx].getLengthUnion()
                                            + columns[cidx].blocks[bidx].getLengthOffset(),
                                    columns[cidx].blocks[bidx].getLengthPayload()));
                    pos2 = buf3.remaining() + buf1.remaining();
                    len2 = buf2.remaining();
                    System.arraycopy(buf2.array(), buf2.position(), data.array(), pos2, len2);
                }
                value = new UnionInputBuffer(data, columns[cidx].blocks[bidx].getRowCount(), unionBits[cidx],
                        unionArray[cidx]);
            }
        } else {
            if (columns[cidx].getCodecName().equals("null")) {
                value = new BlockInputBuffer(ByteBuffer.wrap(raw, 0, len), columns[cidx].blocks[bidx].getRowCount());
            } else if (columns[cidx].blocks[bidx].getLengthOffset() != 0) {
                ByteBuffer data = ByteBuffer.allocate(columns[cidx].blocks[bidx].getUncompressedSize());
                ByteBuffer buf1 = columnValues[cidx].getCodec().decompress(ByteBuffer.wrap(raw,
                        columns[cidx].blocks[bidx].getLengthUnion(), columns[cidx].blocks[bidx].getLengthOffset()));
                System.arraycopy(buf1.array(), 0, data.array(), 0, buf1.limit());
                ByteBuffer buf2 = columnValues[cidx].getCodec()
                        .decompress(ByteBuffer.wrap(raw,
                                columns[cidx].blocks[bidx].getLengthUnion()
                                        + columns[cidx].blocks[bidx].getLengthOffset(),
                                columns[cidx].blocks[bidx].getLengthPayload()));
                System.arraycopy(buf2.array(), buf2.position(), data.array(), buf1.limit(), buf2.remaining());
                value = new BlockInputBuffer(data, columns[cidx].blocks[bidx].getRowCount());
            } else {
                byte[] buf2 = columnValues[cidx].getCodec()
                        .decompress(ByteBuffer.wrap(raw,
                                columns[cidx].blocks[bidx].getLengthUnion()
                                        + columns[cidx].blocks[bidx].getLengthOffset(),
                                columns[cidx].blocks[bidx].getLengthPayload()))
                        .array();
                value = new BlockInputBuffer(ByteBuffer.wrap(buf2), columns[cidx].blocks[bidx].getRowCount());
            }
        }
        /*System.out.println("%%%%c" + cidx + " b" + bidx + " v" + value + " l" + len);*/
        return value;
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            if (terminate) {
                System.exit(0);
            }
            try {
                boolean idle = true;
                long begin = System.currentTimeMillis();
                if (skippingMode) {
                    idle = skippingRead();
                } else {
                    idle = sequentialRead();
                }
                long thisProcessingPeriod = System.currentTimeMillis() - begin;
                if (totalPayload > 0) {
                    int priority =
                            (int) (((totalPayload - processedPayload) * BlockManager.DEFAULT_PRIORITY) / totalPayload)
                                    - (int) Math.log(intendingColumns);
                    if (priority < 0) {
                        priority = 0;
                    }
                    if (priority != currentPriority) {
                        // 5 is the default priority;
                        /*System.out.println(ManagementFactory.getRuntimeMXBean().getName() + " "
                                + (BlockManager.DEFAULT_PRIORITY + priority) + " "
                                + Thread.currentThread().getPriority() + " " + processedPayload + " " + totalPayload
                                + " " + lastProcessingPeriod + " " + period + " " + intendingColumns + " " + priority);*/
                        if (BlockManager.DYNAMIC_PRIORITY) {
                            Thread.currentThread().setPriority(5 + priority);
                        }
                        currentPriority = priority;
                    }
                }
                int balancingPeriod = (int) (BlockManager.BASIC_SLEEP_PERIOD
                        * ((intendingColumns % 10) * BlockManager.DEFAULT_PRIORITY - currentPriority));
                if (idle) {
                    if (BlockManager.DYNAMIC_PRIORITY) {
                        if (balancingPeriod > lastProcessingPeriod) {
                            period += lastProcessingPeriod;
                        } else {
                            period += balancingPeriod;
                        }
                    } else {
                        period += BlockManager.BASIC_SLEEP_PERIOD;
                    }
                } else {
                    if (BlockManager.DYNAMIC_PRIORITY) {
                        if (balancingPeriod > lastProcessingPeriod) {
                            period = (int) lastProcessingPeriod;
                        } else {
                            period = balancingPeriod;
                        }
                        lastProcessingPeriod = thisProcessingPeriod;
                        //intendingColumns++;
                    } else {
                        period = BlockManager.BASIC_SLEEP_PERIOD;
                    }
                }
                /*if (idle) {
                    period += lastProcessingPeriod * (intendingColumns * BlockManager.MAX_PRIORITY
                            + BlockManager.MAX_PRIORITY - currentPriority)
                            / ((1 + intendingColumns) * BlockManager.MAX_PRIORITY);
                } else {
                    period = (int) (lastProcessingPeriod * (intendingColumns * BlockManager.MAX_PRIORITY
                            + BlockManager.MAX_PRIORITY - currentPriority)
                            / ((1 + intendingColumns) * BlockManager.MAX_PRIORITY));
                    lastProcessingPeriod = thisProcessingPeriod;
                }*/
                /*if (idle) {
                period += (BlockManager.CUTOFF_SLEEP_PERIOD * intendingColumns
                        + (BlockManager.MAX_PRIORITY - currentPriority));
                } else {
                    period = BlockManager.BASIC_SLEEP_PERIOD;
                }*/
                if (period > BlockManager.CUTOFF_SLEEP_PERIOD /*|| currentPriority < BlockManager.CUTOFF_PRIORITY*/) {
                    Thread.sleep(period);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

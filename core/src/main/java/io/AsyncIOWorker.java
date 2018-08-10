/**
 * 
 */
package io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

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
    private final BlockColumnValues[] columnValues;
    private final BlockInputBufferQueue[] queues;
    private final Short ioPending;
    private final int[] blocks;
    private final int[] rows;
    private final ColumnDescriptor[] columns;
    private final boolean[] isUnion;
    private final int[] unionBits;
    private final ValueType[][] unionArray;
    private final InputBuffer[] inBuffers;
    private boolean terminate = false;
    private boolean[] intended;
    private BitSet[] valids;
    /*private boolean[] handled;*/
    private boolean isReady = true;
    private final Short ioReady;

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
        /*handled = new boolean[columnValues.length];*/
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
            }
        }
    }

    public BlockColumnValues getColumnValue(int cidx) {
        return columnValues[cidx];
    }

    public void trigger(boolean[] intends, BitSet[] valids) {
        while (!isReady) {
            synchronized (ioReady) {
                try {
                    ioReady.wait();
                } catch (InterruptedException e) {
                    throw new NeciRuntimeException("Cannot trigger when fetching");
                }
            }
        }
        isReady = false;
        this.intended = intends;
        this.valids = valids;
        /*Arrays.fill(handled, false);*/
        for (int i = 0; i < intended.length; i++) {
            if (intended[i]) {
                /*System.out.print(i + ":" + columnValues[i].getName() + " ");*/
                blocks[i] = 0;
                rows[i] = 0;
            }
        }
        /*System.out.println();*/
        synchronized (ioPending) {
            ioPending.notify();
        }
    }

    public void invalid(int cidx) {
        intended[cidx] = false;
        /*handled[cidx] = false;*/
        blocks[cidx] = 0;
        rows[cidx] = 0;
        synchronized (ioPending) {
            ioPending.notify();
        }
        isReady = false;
    }

    public boolean trigger(int cidx, BitSet valid) {
        while (intended[cidx]) {
            try {
                System.out.println("@");
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (columns[cidx].getBlockManager().isFetchingStage()) {
            /*intended[cidx] = true;
            handled[cidx] = false;
            blocks[cidx] = 0;
            rows[cidx] = 0;
            synchronized (ioPending) {
                ioPending.notify();
            }
            isReady = false;*/
            System.out.println("Appending: " + cidx + " name: " + columnValues[cidx].getName());
            if (columnValues[cidx].isArray()) {
                return false;
            } else {
                throw new NeciRuntimeException("Duplicated intended " + cidx + columnValues[cidx].getName());
            }
        }

        if (intended[cidx]) {
            throw new NeciRuntimeException("Duplicated intended " + cidx + columnValues[cidx].getName());
        }

        /*System.out.println("<Trigger: " + cidx + " name: " + columnValues[cidx].getName());*/
        while (!isReady) {
            /*System.out.println("-Trigger: " + cidx + " name: " + columnValues[cidx].getName());*/
            synchronized (ioReady) {
                try {
                    ioReady.wait();
                } catch (InterruptedException e) {
                    throw new NeciRuntimeException("Cannot trigger: " + cidx);
                }
            }
        }
        isReady = false;

        blocks[cidx] = 0;
        rows[cidx] = 0;
        intended[cidx] = true;
        valids[cidx] = valid;
        /*Arrays.fill(handled, false);*/

        synchronized (ioPending) {
            ioPending.notify();
        }
        /*System.out.println(">Trigger: " + cidx + " name: " + columnValues[cidx].getName());*/
        return true;
    }

    public void terminate() {
        terminate = true;
        while (!isReady) {
            synchronized (ioReady) {
                try {
                    ioReady.wait();
                } catch (InterruptedException e) {
                    throw new NeciRuntimeException("Cannot terminate");
                }
            }
        }
        /*for (int i = 0; i < queues.length; i++) {
            if (queues[i] != null) {
                queues[i].clear();
            }
        }*/
        synchronized (ioPending) {
            ioPending.notify();
        }
        isReady = false;
        System.exit(0);
    }

    private int period = 10;

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            if (terminate) {
                System.exit(0);
            }
            try {
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
                            @SuppressWarnings("unchecked")
                            PositionalBlock<Integer, BlockInputBuffer>[] list =
                                    new PositionalBlock[BlockManager.QUEUE_SLOT_DEFAULT_SIZE];
                            for (int k = 0; k < BlockManager.QUEUE_SLOT_DEFAULT_SIZE; k++) {
                                list[k] = new PositionalBlock<Integer, BlockInputBuffer>(blocks[i] + regular,
                                        bufs[regular]);
                                /*System.out.println("\tEnqueue: " + regular + " block: " + (blocks[i] + regular)
                                        + " cidx: " + i + " name: " + columnValues[i].getName() + " total: "
                                        + columnValues[i].getBlockCount());*/
                                regular++;
                            }
                            queues[i].put(list);
                        }
                        if (regular < total) {
                            @SuppressWarnings("unchecked")
                            PositionalBlock<Integer, BlockInputBuffer>[] list = new PositionalBlock[total - regular];
                            for (int k = 0; regular < total; k++) {
                                list[k] = new PositionalBlock<Integer, BlockInputBuffer>(blocks[i] + regular,
                                        bufs[regular]);
                                /*System.out.println("\tEnqueue: " + regular + " block: " + (blocks[i] + regular)
                                        + " cidx: " + i + " name: " + columnValues[i].getName() + " total: "
                                        + columnValues[i].getBlockCount());*/
                                regular++;
                            }
                            queues[i].put(list);
                        }
                        this.blocks[i] += total;
                        if (this.blocks[i] == columnValues[i].getBlockCount()) {
                            /*System.out.println("Completed: " + i + " name: " + columnValues[i].getName());*/
                            intended[i] = false;
                            /*handled[i] = true;*/
                        }
                        /*BlockInputBuffer[] bufs = startBlock(i, count);
                        for (int j = 0; j < count; j++) {
                            queues[i].put(new PositionalBlock<Integer, BlockInputBuffer>(blocks[i] + j, bufs[j]));
                        }
                        this.blocks[i] += count;
                        if (this.blocks[i] == columnValues[i].getBlockCount()) {
                            intended[i] = false;
                            handled[i] = true;
                        }*/
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
                    /*System.out.println("Active");*/
                }
                if (idle) {
                    period += 20;
                } else {
                    period = 0;
                }
                if (period > 10) {
                    Thread.sleep(period);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private BlockInputBuffer[] startBlock(int cidx, int num) throws IOException {
        byte[][] raws = new byte[num][];
        int[] ends = new int[num];
        /*String hint = "";*/
        for (int i = 0; i < num; i++) {
            int block = this.blocks[cidx] + i;
            this.rows[cidx] = columns[cidx].firstRows[block];
            /*int m = valids[cidx].nextSetBit(this.rows[cidx]);
            if (m <= columns[cidx].lastRow(block)) {
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
        /*System.out.println(columnValues[cidx].getName() + " from " + this.blocks[cidx] + " to "
                + (this.blocks[cidx] + num) + " hint " + hint);*/
        /*int total = 0;
        int[] ends = new int[num];
        for (int i = 0; i < num; i++) {
            int block = this.blocks[cidx] + i;
            ends[i] = columns[cidx].blocks[block].getCompressedSize();
            total += ends[i] + columnValues[cidx].getChecksum().size();
        }
        byte[] longrun = new byte[total];
        inBuffers[cidx].readFully(longrun);
        byte[][] raws = new byte[num][];
        int offset = 0;
        for (int i = 0; i < num; i++) {
            if (i > 0) {
                offset += ends[i - 1];
            }
            raws[i] = ByteBuffer.wrap(longrun, offset, ends[i]).array();
        }*/

        BlockInputBuffer[] values = new BlockInputBuffer[num];
        long beginCompression = System.nanoTime();
        for (int i = 0; i < num; i++) {
            int block = this.blocks[cidx] + i;
            if (isUnion[cidx]) {
                if (columns[cidx].getCodecName().equals("null")) {
                    values[i] = new UnionInputBuffer(ByteBuffer.wrap(raws[i], 0, ends[i]),
                            columns[cidx].blocks[block].getRowCount(), unionBits[cidx], unionArray[cidx]);
                } else {
                    ByteBuffer data = null;
                    if (columns[cidx].blocks[block].getUncompressedSize() >= columns[cidx].blocks[block]
                            .getCompressedSize()) {
                        data = ByteBuffer.allocate(columns[cidx].blocks[block].getUncompressedSize());
                    } else {
                        data = ByteBuffer.allocate(columns[cidx].blocks[block].getCompressedSize());
                    }
                    ByteBuffer buf3 = columnValues[cidx].getCodec()
                            .decompress(ByteBuffer.wrap(raws[i], 0, columns[cidx].blocks[block].getLengthUnion()));
                    int pos0 = 0;
                    int len0 = buf3.limit();
                    System.arraycopy(buf3.array(), 0, data.array(), pos0, len0);
                    ByteBuffer buf1 = columnValues[cidx].getCodec()
                            .decompress(ByteBuffer.wrap(raws[i], columns[cidx].blocks[block].getLengthUnion(),
                                    columns[cidx].blocks[block].getLengthOffset()));
                    int pos1 = buf3.remaining();
                    int len1 = buf1.remaining();
                    System.arraycopy(buf1.array(), buf1.position(), data.array(), pos1, len1);
                    int pos2 = -1;
                    int len2 = -1;
                    if (columns[cidx].blocks[block].getLengthPayload() != 0) {
                        ByteBuffer buf2 = columnValues[cidx].getCodec()
                                .decompress(ByteBuffer.wrap(raws[i],
                                        columns[cidx].blocks[block].getLengthUnion()
                                                + columns[cidx].blocks[block].getLengthOffset(),
                                        columns[cidx].blocks[block].getLengthPayload()));
                        pos2 = buf3.remaining() + buf1.remaining();
                        len2 = buf2.remaining();
                        System.arraycopy(buf2.array(), buf2.position(), data.array(), pos2, len2);
                    }
                    values[i] = new UnionInputBuffer(data, columns[cidx].blocks[block].getRowCount(), unionBits[cidx],
                            unionArray[cidx]);
                }
            } else {
                if (columns[cidx].getCodecName().equals("null")) {
                    values[i] = new BlockInputBuffer(ByteBuffer.wrap(raws[i], 0, ends[i]),
                            columns[cidx].blocks[block].getRowCount());
                } else if (columns[cidx].blocks[block].getLengthOffset() != 0) {
                    ByteBuffer data = ByteBuffer.allocate(columns[cidx].blocks[block].getUncompressedSize());
                    ByteBuffer buf1 = columnValues[cidx].getCodec()
                            .decompress(ByteBuffer.wrap(raws[i], columns[cidx].blocks[block].getLengthUnion(),
                                    columns[cidx].blocks[block].getLengthOffset()));
                    System.arraycopy(buf1.array(), 0, data.array(), 0, buf1.limit());
                    ByteBuffer buf2 = columnValues[cidx].getCodec()
                            .decompress(ByteBuffer.wrap(raws[i],
                                    columns[cidx].blocks[block].getLengthUnion()
                                            + columns[cidx].blocks[block].getLengthOffset(),
                                    columns[cidx].blocks[block].getLengthPayload()));
                    System.arraycopy(buf2.array(), buf2.position(), data.array(), buf1.limit(), buf2.remaining());
                    values[i] = new BlockInputBuffer(data, columns[cidx].blocks[block].getRowCount());
                } else {
                    byte[] buf2 = columnValues[cidx].getCodec()
                            .decompress(ByteBuffer.wrap(raws[i],
                                    columns[cidx].blocks[block].getLengthUnion()
                                            + columns[cidx].blocks[block].getLengthOffset(),
                                    columns[cidx].blocks[block].getLengthPayload()))
                            .array();
                    values[i] = new BlockInputBuffer(ByteBuffer.wrap(buf2), columns[cidx].blocks[block].getRowCount());
                }
            }
        }
        columns[cidx].getBlockManager().compressionTimeAdd(System.nanoTime() - beginCompression);
        return values;
    }
}

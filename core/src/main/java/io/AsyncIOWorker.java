/**
 * 
 */
package io;

import java.io.IOException;
import java.nio.ByteBuffer;

import columnar.BlockColumnValues;
import columnar.ColumnDescriptor;
import misc.ValueType;

/**
 * @author Michael
 *
 */
public class AsyncIOWorker implements Runnable {
    private static final int BUFLEN = 4096;
    private static final int ROUND = 1048576;
    private final BlockColumnValues[] columnValues;
    private final BlockInputBufferQueue[] queue;
    private final int[] blocks;
    private final int[] rows;
    private final ColumnDescriptor[] columns;
    private final boolean[] isUnion;
    private final int[] unionBits;
    private final ValueType[][] unionArray;
    private final InputBuffer[] inBuffers;
    private boolean[] intended;
    private int count = 0;
    /*private Random random = new Random(47);*/
    private long position = 0;

    public AsyncIOWorker(BlockColumnValues[] columnValues, BlockInputBufferQueue[] queue) throws IOException {
        this.columnValues = columnValues;
        this.blocks = new int[columnValues.length];
        this.rows = new int[columnValues.length];
        this.queue = queue;
        this.inBuffers = new InputBuffer[columnValues.length];
        this.isUnion = new boolean[columnValues.length];
        this.unionBits = new int[columnValues.length];
        this.unionArray = new ValueType[columnValues.length][];
        this.columns = new ColumnDescriptor[columnValues.length];
        for (int i = 0; i < columnValues.length; i++) {
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

    public void trigger(boolean[] intends) {
        this.intended = intends;
    }

    public BlockInputBuffer startBlock(int cidx, int block) throws IOException {
        BlockInputBuffer values = null;
        this.blocks[cidx] = block;
        this.rows[cidx] = columns[cidx].firstRows[block];

        inBuffers[cidx].seek(columns[cidx].blockStarts[block]);
        int end = columns[cidx].blocks[block].getCompressedSize();
        byte[] raw = new byte[end + columnValues[cidx].getChecksum().size()];
        inBuffers[cidx].readFully(raw);
        long beginCompression = System.nanoTime();
        if (isUnion[cidx]) {
            if (columns[cidx].getCodecName().equals("null")) {
                values = new UnionInputBuffer(ByteBuffer.wrap(raw, 0, end), columns[cidx].blocks[block].getRowCount(),
                        unionBits[cidx], unionArray[cidx]);
            } else {
                ByteBuffer data = null;
                if (columns[cidx].blocks[block].getUncompressedSize() >= columns[cidx].blocks[block]
                        .getCompressedSize()) {
                    data = ByteBuffer.allocate(columns[cidx].blocks[block].getUncompressedSize());
                } else {
                    data = ByteBuffer.allocate(columns[cidx].blocks[block].getCompressedSize());
                }
                ByteBuffer buf3 = columnValues[cidx].getCodec()
                        .decompress(ByteBuffer.wrap(raw, 0, columns[cidx].blocks[block].getLengthUnion()));
                int pos0 = 0;
                int len0 = buf3.limit();
                //System.out.println("\t" + pos0 + ":" + len0 + " with:" + column.blocks[block].lengthUnion);
                System.arraycopy(buf3.array(), 0, data.array(), pos0, len0);
                ByteBuffer buf1 = columnValues[cidx].getCodec().decompress(ByteBuffer.wrap(raw,
                        columns[cidx].blocks[block].getLengthUnion(), columns[cidx].blocks[block].getLengthOffset()));
                int pos1 = buf3.remaining();
                int len1 = buf1.remaining();
                System.arraycopy(buf1.array(), buf1.position(), data.array(), pos1, len1);
                int pos2 = -1;
                int len2 = -1;
                if (columns[cidx].blocks[block].getLengthPayload() != 0) {
                    ByteBuffer buf2 = columnValues[cidx].getCodec()
                            .decompress(ByteBuffer.wrap(raw,
                                    columns[cidx].blocks[block].getLengthUnion()
                                            + columns[cidx].blocks[block].getLengthOffset(),
                                    columns[cidx].blocks[block].getLengthPayload()));
                    pos2 = buf3.remaining() + buf1.remaining();
                    len2 = buf2.remaining();
                    System.arraycopy(buf2.array(), buf2.position(), data.array(), pos2, len2);
                }
                values = new UnionInputBuffer(data, columns[cidx].blocks[block].getRowCount(), unionBits[cidx],
                        unionArray[cidx]);
            }
        } else {
            if (columns[cidx].getCodecName().equals("null")) {
                values = new BlockInputBuffer(ByteBuffer.wrap(raw, 0, end), columns[cidx].blocks[block].getRowCount());
            } else if (columns[cidx].blocks[block].getLengthOffset() != 0) {
                ByteBuffer data = ByteBuffer.allocate(columns[cidx].blocks[block].getUncompressedSize());
                ByteBuffer buf1 = columnValues[cidx].getCodec().decompress(ByteBuffer.wrap(raw,
                        columns[cidx].blocks[block].getLengthUnion(), columns[cidx].blocks[block].getLengthOffset()));
                System.arraycopy(buf1.array(), 0, data.array(), 0, buf1.limit());
                ByteBuffer buf2 = columnValues[cidx].getCodec()
                        .decompress(ByteBuffer.wrap(raw,
                                columns[cidx].blocks[block].getLengthUnion()
                                        + columns[cidx].blocks[block].getLengthOffset(),
                                columns[cidx].blocks[block].getLengthPayload()));
                System.arraycopy(buf2.array(), buf2.position(), data.array(), buf1.limit(), buf2.remaining());
                values = new BlockInputBuffer(data, columns[cidx].blocks[block].getRowCount());
            } else {
                byte[] buf2 = columnValues[cidx].getCodec()
                        .decompress(ByteBuffer.wrap(raw,
                                columns[cidx].blocks[block].getLengthUnion()
                                        + columns[cidx].blocks[block].getLengthOffset(),
                                columns[cidx].blocks[block].getLengthPayload()))
                        .array();
                values = new BlockInputBuffer(ByteBuffer.wrap(buf2), columns[cidx].blocks[block].getRowCount());
            }
        }
        columns[cidx].getBlockManager().compressionTimeAdd(System.nanoTime() - beginCompression);
        return values;
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                if (count >= ROUND) {
                    System.exit(0);
                }
                for (int i = 0; i < queue.length; i++) {
                    if (!intended[i]) {
                        continue;
                    }
                    System.out.println(position);
                    BlockInputBuffer buf = startBlock(i, -1);
                    queue[i].put(new PositionalBlock<Long, BlockInputBuffer>(position, buf));
                    position += buf.runLength;
                }
                count++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

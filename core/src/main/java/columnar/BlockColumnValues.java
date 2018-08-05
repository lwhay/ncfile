/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package columnar;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.trevni.TrevniRuntimeException;

import codec.Checksum;
import codec.Codec;
import io.BlockInputBuffer;
import io.InputBuffer;
import io.UnionInputBuffer;
import misc.ValueType;

/**
 * An iterator over column values.
 */
public class BlockColumnValues<T extends Comparable> implements Iterator<T>, Iterable<T> {

    protected final ColumnDescriptor column;
    protected final ValueType type;
    protected final Codec codec;
    protected final Checksum checksum;
    protected final InputBuffer in;

    protected BlockInputBuffer values;
    protected int block = -1;
    protected int row = 0;
    protected T previous;
    protected int offset = 0;

    protected int arrayLength;
    //    protected long time;
    //    protected int readBlockSize;
    //    protected int seekedBlock;
    //    protected List<Long> blockTime;
    //    protected List<Long> blockStart;
    //    protected List<Long> blockEnd;
    //    protected List<Long> blockOffset;

    protected boolean isUnion;
    protected int unionBits;
    protected ValueType[] unionArray;

    protected BlockColumnValues(ColumnDescriptor column) throws IOException {
        this.column = column;
        this.type = column.metaData.getType();
        this.codec = Codec.get(column.metaData);
        this.checksum = Checksum.get(column.metaData);
        this.in = new InputBuffer(column.getBlockManager(), column.dataFile);

        if (type.equals(ValueType.UNION)) {
            isUnion = true;
            unionBits = column.metaData.getUnionBits();
            unionArray = column.metaData.getUnionArray();
        }

        column.ensureBlocksRead();
    }

    /**
     * Return the current row number within this file.
     */
    public int getRow() {
        return row;
    }

    public int getLastRow() {
        return column.lastRow();
    }

    public ValueType getType() {
        return type;
    }

    public String getName() {
        return column.metaData.getName();
    }

    public String getParentName() {
        if (column.metaData.getParent() != null)
            return column.metaData.getParent().getName();
        else
            return null;
    }

    public int getLayer() {
        return column.metaData.getLayer();
    }

    public boolean isArray() {
        return column.metaData.isArray();
    }

    public void create() throws IOException {
        offset = 0;
        seek(0);
    }

    //    public void createTime() {
    //        time = 0;
    //        blockTime = new ArrayList<Long>();
    //        blockStart = new ArrayList<Long>();
    //        blockEnd = new ArrayList<Long>();
    //        blockOffset = new ArrayList<Long>();
    //    }
    //
    //    public long getTime() {
    //        return time;
    //    }

    //    public List<Long> getBlockTime() {
    //        return blockTime;
    //    }
    //
    //    public List<Long> getBlockStart() {
    //        return blockStart;
    //    }
    //
    //    public List<Long> getBlockEnd() {
    //        return blockEnd;
    //    }
    //
    //    public List<Long> getBlockOffset() {
    //        return blockOffset;
    //    }

    //    public void createSeekBlock() {
    //        readBlockSize = 0;
    //        seekedBlock = 0;
    //    }
    //
    //    public int[] getSeekBlock() {
    //        return new int[] { readBlockSize, seekedBlock };
    //    }

    public int getBlockCount() {
        return column.blockCount();
    }

    /**
     * Seek to the named row.
     */
    public void seek(int r) throws IOException {
        if (r < row || r >= column.lastRow(block)) // not in current block
            startBlock(column.findBlock(r)); // seek to block start
        if (r > row) { // skip within block
            if (column.metaData.isArray())
                values.skipLength(r - row);
            else
                values.skipValue(type, r - row);
            row = r;
        }
        previous = null;
    }

    public void turnTo(int r) throws IOException {
        if (r < row || r >= column.lastRow(block)) // not in current block
            seekBlock(column.findBlock(r)); // seek to block start
        if (r > row) { // skip within block
            if (column.metaData.isArray())
                values.skipLength(r - row);
            else
                values.skipValue(type, r - row);
            row = r;
        }
        previous = null;
    }

    public void seekBlock(int block) throws IOException {
        for (int i = this.block + 1; i < block; i++) {
            in.seek(column.blockStarts[i]);
            int end = column.blocks[i].compressedSize;
            byte[] raw = new byte[end + checksum.size()];
            in.readFully(raw);
        }
        startBlock(block);
    }

    public void startBlock(int block) throws IOException {
        //long s = System.nanoTime();
        //        readBlockSize++;
        //        seekedBlock += Math.abs(block - this.block - 1);
        //                if (skipLength != null) {
        //        if (this.block == -1)
        //            skipLength.add(column.blockStarts[block] - column.start);
        //        else
        //            skipLength.add(column.blockStarts[block] - column.blockStarts[this.block]
        //                    - column.blocks[this.block].compressedSize - checksum.size());
        //                }
        this.block = block;
        this.row = column.firstRows[block];

        in.seek(column.blockStarts[block]);
        int end = column.blocks[block].getCompressedSize();
        //System.out.println(column.metaData.getName() + "\t:" + block + "\t" + column.blockStarts[block] + "\t" + end);
        byte[] raw = new byte[end + checksum.size()];
        in.readFully(raw);
        /*ByteBuffer data = codec.decompress(ByteBuffer.wrap(raw, 0, end));
        if (!checksum.compute(data).equals(ByteBuffer.wrap(raw, end, checksum.size())))
            throw new IOException("Checksums mismatch.");*/
        long beginCompression = System.nanoTime();
        if (isUnion) {
            if (column.getCodecName().equals("null")) {
                values = new UnionInputBuffer(ByteBuffer.wrap(raw, 0, end), column.blocks[block].rowCount, unionBits,
                        unionArray);
            } else {
                ByteBuffer data = null;
                if (column.blocks[block].getUncompressedSize() >= column.blocks[block].getCompressedSize()) {
                    data = ByteBuffer.allocate(column.blocks[block].getUncompressedSize());
                } else {
                    data = ByteBuffer.allocate(column.blocks[block].getCompressedSize());
                }
                /*System.out.println("\t" + column.blocks[block].lengthUnion + ":" + column.blocks[block].lengthOffset + ":"
                    + column.blocks[block].lengthPayload + ":" + column.blocks[block].getCompressedSize() + ":"
                    + column.blocks[block].getUncompressedSize());*/
                ByteBuffer buf3 = codec.decompress(ByteBuffer.wrap(raw, 0, column.blocks[block].lengthUnion));
                int pos0 = 0;
                int len0 = buf3.limit();
                //System.out.println("\t" + pos0 + ":" + len0 + " with:" + column.blocks[block].lengthUnion);
                System.arraycopy(buf3.array(), 0, data.array(), pos0, len0);
                ByteBuffer buf1 = codec.decompress(
                        ByteBuffer.wrap(raw, column.blocks[block].lengthUnion, column.blocks[block].lengthOffset));
                int pos1 = buf3.remaining();
                int len1 = buf1.remaining();
                System.arraycopy(buf1.array(), buf1.position(), data.array(), pos1, len1);
                int pos2 = -1;
                int len2 = -1;
                if (column.blocks[block].lengthPayload != 0) {
                    ByteBuffer buf2 = codec.decompress(
                            ByteBuffer.wrap(raw, column.blocks[block].lengthUnion + column.blocks[block].lengthOffset,
                                    column.blocks[block].lengthPayload));
                    pos2 = buf3.remaining() + buf1.remaining();
                    len2 = buf2.remaining();
                    System.arraycopy(buf2.array(), buf2.position(), data.array(), pos2, len2);
                }
                //System.out.println(pos0 + ":" + len0 + "-" + pos1 + ":" + len1 + "-" + pos2 + ":" + len2);
                values = new UnionInputBuffer(data, column.blocks[block].rowCount, unionBits, unionArray);
            }
        } else {
            if (column.getCodecName().equals("null")) {
                values = new BlockInputBuffer(ByteBuffer.wrap(raw, 0, end), column.blocks[block].rowCount);
            } else if (column.blocks[block].lengthOffset != 0) {
                ByteBuffer data = ByteBuffer.allocate(column.blocks[block].getUncompressedSize());
                ByteBuffer buf1 = codec.decompress(
                        ByteBuffer.wrap(raw, column.blocks[block].lengthUnion, column.blocks[block].lengthOffset));
                System.arraycopy(buf1.array(), 0, data.array(), 0, buf1.limit());
                ByteBuffer buf2 = codec.decompress(
                        ByteBuffer.wrap(raw, column.blocks[block].lengthUnion + column.blocks[block].lengthOffset,
                                column.blocks[block].lengthPayload));
                System.arraycopy(buf2.array(), buf2.position(), data.array(), buf1.limit(), buf2.remaining());
                values = new BlockInputBuffer(data, column.blocks[block].rowCount);
            } else {
                byte[] buf2 = codec.decompress(
                        ByteBuffer.wrap(raw, column.blocks[block].lengthUnion + column.blocks[block].lengthOffset,
                                column.blocks[block].lengthPayload))
                        .array();
                values = new BlockInputBuffer(ByteBuffer.wrap(buf2), column.blocks[block].rowCount);
            }
        }
        column.getBlockManager().compressionTimeAdd(System.nanoTime() - beginCompression);
        //long e = System.nanoTime();
        //        blockTime.add((e - s));
        //        blockStart.add(s);
        //        blockEnd.add(e);
        //        blockOffset.add(column.blockStarts[block]);
        //        time += e - s;
    }

    @Override
    public Iterator iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return block < column.blockCount() - 1 || row < column.lastRow(block);
    }

    @Override
    public T next() {
        if (column.metaData.isArray())
            throw new TrevniRuntimeException("Column is array: " + column.metaData.getName());
        try {
            startRow();
            return nextValue();
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
    }

    public void readIO() throws IOException {
        for (int i = 0; i < column.blockCount(); i++) {
            startBlock(i);
        }
    }

    /**
     * Expert: Must be called before any calls to {@link #nextLength()} or
     * {@link #nextValue()}.
     */
    public void startRow() throws IOException {
        if (row >= column.lastRow(block)) {
            if (block >= column.blockCount())
                throw new TrevniRuntimeException("Read past end of column.");
            startBlock(block + 1);
        }
        row++;
    }

    /**
     * Expert: Returns the next length in an array column.
     */
    public int nextLength() {
        if (!column.metaData.isArray())
            throw new TrevniRuntimeException("Column is not array: " + column.metaData.getName());
        assert arrayLength == 0;
        try {
            offset = arrayLength = values.readLength();
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
        return arrayLength;
    }

    /*
     * while the array column is incrementally stored, return the array Length and the first offset.
     */
    public int[] nextLengthAndOffset() {
        if (!column.metaData.isArray())
            throw new TrevniRuntimeException("Column is not array: " + column.metaData.getName());
        assert arrayLength == 0;
        int[] res = new int[2];
        res[1] = offset;
        try {
            offset = values.readLength();
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
        res[0] = arrayLength = offset - res[1];
        return res;
    }

    /**
     * Expert: Returns the next value in a column.
     */
    public T nextValue() throws IOException {
        arrayLength--;
        return previous = values.<T> readValue(type);
    }

    public void skipValue(int r) throws IOException {
        values.skipValue(type, r);
    }

    public int nextKey() throws IOException {
        arrayLength--;
        return values.readFixed32();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}

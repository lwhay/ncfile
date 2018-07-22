package io;

import java.io.IOException;
import java.nio.ByteBuffer;

import columnar.ColumnDescriptor;
import columnar.ColumnValues;
import misc.InputBytes;

public class MidColumnValues<T extends Comparable> extends ColumnValues<T> {
    protected MidColumnValues(ColumnDescriptor column) throws IOException {
        super(column);
    }

    @Override
    public void startBlock(int block) throws IOException {
        this.block = block;
        this.row = column.firstRows[block];

        in.seek(column.blockStarts[block]);
        int end = column.blocks[block].compressedSize;
        byte[] raw = new byte[end + checksum.size()];
        in.readFully(raw);
        ByteBuffer data = codec.decompress(ByteBuffer.wrap(raw, 0, end));
        if (!checksum.compute(data).equals(ByteBuffer.wrap(raw, end, checksum.size())))
            throw new IOException("Checksums mismatch.");
        values = new MidInputBuffer(column.getBlockManager(), new InputBytes(data));
    }
}

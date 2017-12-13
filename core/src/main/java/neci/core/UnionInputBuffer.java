package neci.core;

import java.io.IOException;
import java.nio.ByteBuffer;

public class UnionInputBuffer extends BlockInputBuffer {
    private byte[] buf3;
    private int oneUnion;
    private int count;
    private ValueType[] unionTypes;
    BlockInputBuffer buf;
    private int bitCount;
    int off;
    int xx;

    public UnionInputBuffer(ByteBuffer data, int count, int oneUnion, ValueType[] unionTypes) {
        this.oneUnion = oneUnion;
        switch (oneUnion) {
            case 1:
                xx = 0x01;
                break;
            case 2:
                xx = 0x03;
                break;
            case 4:
                xx = 0x0f;
                break;
            case 8:
                xx = 0xff;
        }
        this.count = count;
        this.unionTypes = unionTypes;
        int length = oneUnion * count / 8;
        buf3 = new byte[length];
        data.get(buf3, 0, length);
        buf = new BlockInputBuffer(data, count);
    }

    @Override
    public <T extends Comparable> T readValue(ValueType type) throws IOException {
        int i = buf3[off] >> bitCount;
        i &= xx;
        bitCount += oneUnion;
        if (bitCount == 8) {
            bitCount = 0;
            off++;
        }
        T r = buf.readValue(unionTypes[i]);
        if (UnionOutputBuffer.isFixed(unionTypes[i]))
            buf.skipNull();
        return r;
    }

    @Override
    public void skipValue(ValueType type, int r) throws IOException {
        off += r * oneUnion / 8;
        bitCount += (r * oneUnion) % 8;
        if (bitCount >= 8) {
            off++;
            bitCount -= 8;
        }
        buf.skipBytes(r);
    }
}

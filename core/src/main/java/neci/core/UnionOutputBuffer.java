package neci.core;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.trevni.TrevniRuntimeException;

public class UnionOutputBuffer extends BlockOutputBuffer {
    private byte[] buf3;
    private int union;
    private ValueType[] unionTypes;
    private int count3;
    private int bitCount3;
    private int oneUnion;

    public UnionOutputBuffer(ValueType[] types) {
        super();
        this.union = types.length;
        buf3 = new byte[COUNT];
        unionTypes = types;
        if (union <= 2)
            oneUnion = 1;
        else if (union <= 4)
            oneUnion = 2;
        else if (union <= 16)
            oneUnion = 4;
        else if (union <= 32)
            oneUnion = 8;
    }

    public boolean isFull() {
        return (count1 + count2 + count3) >= BLOCK_SIZE;
    }

    public int size() {
        return count1 + count2 + count3;
    }

    public void close() {
        super.close();
        buf3 = null;
    }

    public void writeValue(Object value, ValueType type) throws IOException {
        int i = 0;
        for (; i < unionTypes.length; i++) {
            if (type.equals(unionTypes[i]))
                break;
        }
        if (i >= unionTypes.length)
            throw new TrevniRuntimeException("Illegal value type: " + type);
        else {
            writeUnion(i);
            super.writeValue(value, type);
        }
    }

    private void writeUnion(int i) {
        if (bitCount3 == 0) {
            ensureUnion(1);
            count3++;
        }
        buf3[count3 - 1] |= ((byte) (i & 0xff)) << (bitCount3 + 8 - oneUnion);
        bitCount3 += oneUnion;
        if (bitCount3 == 8)
            bitCount3 = 0;
    }

    private void ensureUnion(int n) {
        if (count3 + n > buf3.length)
            buf3 = Arrays.copyOf(buf3, Math.max(buf3.length << 1, count3 + n));
    }

    public synchronized void writeTo(OutputStream out) throws IOException {
        out.write(buf3, 0, count3);
        out.write(buf1, 0, count1);
        out.write(buf2, 0, count2);
    }

    public synchronized void reset() {
        count3 = 0;
        count1 = 0;
        count2 = 0;
    }
}

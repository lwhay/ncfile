package org.apache.trevni.update;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.trevni.TrevniRuntimeException;

public class MidOutputBuffer extends OutputBuffer {
    public MidOutputBuffer() {
        super();
    }

    @Override
    public void writeValue(Object value, ValueType type) throws IOException {
        switch (type) {
            case NULL:
                break;
            case BOOLEAN:
                if (value == null) {
                    writeBoolean(false);
                    return;
                } else {
                    writeBoolean(true);
                    writeBoolean((Boolean) value);
                }
                break;
            case INT:
                if (value == null) {
                    writeByte((byte) 1);
                    return;
                } else {
                    writeByte((byte) 0);
                    writeFixed32((Integer) value);
                }
                break;
            case LONG:
                if (value == null) {
                    writeByte((byte) 1);
                    return;
                } else {
                    writeByte((byte) 0);
                    writeFixed64((Long) value);
                }
                break;
            case FIXED32:
                if (value == null) {
                    writeByte((byte) 1);
                    return;
                } else {
                    writeByte((byte) 0);
                    writeFixed32((Integer) value);
                }
                break;
            case FIXED64:
                if (value == null) {
                    writeByte((byte) 1);
                    return;
                } else {
                    writeByte((byte) 0);
                    writeFixed64((Long) value);
                }
                break;
            case FLOAT:
                if (value == null) {
                    writeByte((byte) 1);
                    return;
                } else {
                    writeByte((byte) 0);
                    writeFloat((Float) value);
                }
                break;
            case DOUBLE:
                if (value == null) {
                    writeByte((byte) 1);
                    return;
                } else {
                    writeByte((byte) 0);
                    writeDouble((Double) value);
                }
                break;
            case STRING:
                if (value == null) {
                    writeByte((byte) 1);
                    return;
                } else {
                    writeByte((byte) 0);
                    writeString((String) value);
                }
                break;
            case BYTES:
                if (value == null) {
                    writeByte((byte) 1);
                    return;
                } else {
                    writeByte((byte) 0);
                    if (value instanceof ByteBuffer)
                        writeBytes((ByteBuffer) value);
                    else
                        writeBytes((byte[]) value);
                }
                break;
            default:
                throw new TrevniRuntimeException("Unknown value type: " + type);
        }
    }

    public void writeKeyGroup(byte flag, int[] keys) throws IOException {
        writeByte(flag);
        for (int k : keys)
            writeFixed32(k);
    }

    public void writeByte(byte v) throws IOException {
        ensure(1);
        buf[count] = v;
        count++;
    }

    @Override
    public void writeLength(int length) throws IOException {
        bitCount = 0;
        if (length < 0)
            writeBoolean(false);
        else {
            writeBoolean(true);
            writeFixed32(length);
        }
    }
}

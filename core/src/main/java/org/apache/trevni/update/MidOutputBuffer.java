package org.apache.trevni.update;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.trevni.TrevniRuntimeException;
import org.apache.trevni.ValueType;

public class MidOutputBuffer extends OutputBuffer {
    public MidOutputBuffer() {
        super();
    }

    @Override
    public void writeValue(Object value, ValueType type) throws IOException {
        if (value == null) {
            writeBoolean(false);
            return;
        } else
            writeBoolean(true);
        switch (type) {
            case NULL:
                break;
            case BOOLEAN:
                writeBoolean((Boolean) value);
                break;
            case INT:
                writeFixed32((Integer) value);
                break;
            case LONG:
                writeFixed64((Long) value);
                break;
            case FIXED32:
                writeFixed32((Integer) value);
                break;
            case FIXED64:
                writeFixed64((Long) value);
                break;
            case FLOAT:
                writeFloat((Float) value);
                break;
            case DOUBLE:
                writeDouble((Double) value);
                break;
            case STRING:
                writeString((String) value);
                break;
            case BYTES:
                if (value instanceof ByteBuffer)
                    writeBytes((ByteBuffer) value);
                else
                    writeBytes((byte[]) value);
                break;
            default:
                throw new TrevniRuntimeException("Unknown value type: " + type);
        }
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

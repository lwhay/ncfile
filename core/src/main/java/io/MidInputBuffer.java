package io;

import java.io.IOException;

import org.apache.trevni.Input;
import org.apache.trevni.TrevniRuntimeException;

import columnar.BlockManager;
import misc.KeyGroup;
import misc.ValueType;

public class MidInputBuffer extends InputBuffer {

    public MidInputBuffer(BlockManager bm, Input in) throws IOException {
        super(bm, in);
    }

    public MidInputBuffer(BlockManager bm, Input in, int position) throws IOException {
        super(bm, in, position);
    }

    @Override
    public <T extends Comparable> T readValue(ValueType type) throws IOException {
        switch (type) {
            case NULL:
                return (T) null;
            case BOOLEAN:
                if (readBoolean())
                    return (T) Boolean.valueOf(readBoolean());
                else
                    return null;
            case INT:
                if (readByte() == 1)
                    return (T) Integer.valueOf(readFixed32());
                else
                    return null;
            case LONG:
                if (readByte() == 1)
                    return (T) Long.valueOf(readFixed64());
                else
                    return null;
            case FIXED32:
                if (readByte() == 1)
                    return (T) Integer.valueOf(readFixed32());
                else
                    return null;
            case FIXED64:
                if (readByte() == 1)
                    return (T) Long.valueOf(readFixed64());
                else
                    return null;
            case FLOAT:
                if (readByte() == 1)
                    return (T) Float.valueOf(readFloat());
                else
                    return null;
            case DOUBLE:
                if (readByte() == 1)
                    return (T) Double.valueOf(readDouble());
                else
                    return null;
            case STRING:
                if (readByte() == 1)
                    return (T) readString();
                else
                    return null;
            case BYTES:
                if (readByte() == 1)
                    return (T) readBytes(null);
                else
                    return null;
            default:
                throw new TrevniRuntimeException("Unknown value type: " + type);
        }
    }

    @Override
    public void skipValue(ValueType type) throws IOException {
        switch (type) {
            case NULL:
                break;
            case BOOLEAN:
                if (readBoolean())
                    readBoolean();
                break;
            case INT:
                //                if (readBoolean())
                //                    readFixed32();
                //                break;
                //            case LONG:
                //                if (readBoolean())
                //                    readFixed64();
                //                break;
            case FIXED32:
            case FLOAT:
                if (readBoolean())
                    skip(4);
                break;
            case LONG:
            case FIXED64:
            case DOUBLE:
                if (readBoolean())
                    skip(8);
                break;
            case STRING:
            case BYTES:
                if (readBoolean())
                    skipBytes();
                break;
            default:
                throw new TrevniRuntimeException("Unknown value type: " + type);
        }
    }

    public int readByte() throws IOException {
        return read();
    }

    public KeyGroup readKeyGroup(int len) throws IOException {
        byte flag = (byte) readByte();
        int[] res = new int[len];
        for (int i = 0; i < len; i++)
            res[i] = readFixed32();
        return new KeyGroup(flag, res);
    }

    @Override
    public int readLength() throws IOException {
        bitCount = 0;
        if (runLength > 0) {
            runLength--; // in run
            return runValue;
        }

        if (!readBoolean())
            return -1;
        int length = readFixed32();
        if (length >= 0) // not a run
            return length;

        runLength = (1 - length) >>> 1; // start of run
        runValue = (length + 1) & 1;
        return runValue;
    }
}

package neci.ncfile.generic;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import exceptions.NeciRuntimeException;
import misc.GroupCore;
import neci.ncfile.base.Schema;
import neci.ncfile.base.Schema.Field;
import neci.ncfile.generic.GenericData.Group;
import neci.ncfile.generic.GenericData.Record;

public class GenericGroupReader {
    GenericData data = new GenericData();
    byte[] buf;
    int pos;
    int limit;
    Schema schema;

    public Group readGroup(GroupCore buffer, Schema schema) {
        this.schema = schema;
        this.buf = buffer.array();
        this.limit = buf.length;
        pos = 0;
        Group res = new Group(schema);
        int i = 0;
        for (Field f : schema.getFields()) {
            res.put(i++, readField(f));
        }
        return res;
    }

    public Record readRecord(Schema schema) {
        Record res = new Record(schema);
        int i = 0;
        for (Field f : schema.getFields()) {
            res.put(i++, readField(f));
        }
        return res;
    }

    public Object readField(Field f) {
        return read(f.schema());
    }

    public Object read(Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                return readRecord(schema);
            //            case ENUM:
            //                return readEnum(expected, in);
            case ARRAY:
                return readArray(schema);
            //            case MAP:
            //                return readMap(old, expected, in);
            case UNION:
                return read(schema);
            //            case FIXED:
            //                return readFixed(old, expected, in);
            case STRING:
                return readString();
            case BYTES:
                return readBytes();
            case INT:
                return readInt();
            case LONG:
                return readLong();
            case FLOAT:
                return readFloat();
            case DOUBLE:
                return readDouble();
            case BOOLEAN:
                return readBoolean();
            case NULL:
                readNull();
                return null;
            default:
                throw new NeciRuntimeException("Unknown type: " + schema);
        }
    }

    public Object readArray(Schema schema) {
        int len = readInt();
        Schema element = schema.getElementType();
        if (len > 0) {
            Collection array = new GenericData.Array(len, schema);
            for (int i = 0; i < len; i++) {
                array.add(read(element));
            }
            return array;
        } else {
            return new GenericData.Array(0, schema);
        }
    }

    public Object readUnion(Schema schema) {
        ensureBounds(1);
        int index = buf[pos++] & 0xff;
        return read(schema.getTypes().get(index));
    }

    public String readString() {
        int len = readInt();
        ensureBounds(len);
        String res = new String(buf, pos, len);
        pos += len;
        return res;
    }

    public ByteBuffer readBytes() {
        int len = readInt();
        ensureBounds(len);
        ByteBuffer res = ByteBuffer.allocate(len);
        System.arraycopy(buf, pos, res, 0, len);
        pos += len;
        return res;
    }

    public int readInt() {
        ensureBounds(4);
        int len = 1;
        int n = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8) | ((buf[pos + len++] & 0xff) << 16)
                | ((buf[pos + len++] & 0xff) << 24);
        pos += 4;
        return n;
    }

    public long readLong() {
        int len = 1;
        int n1 = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8) | ((buf[pos + len++] & 0xff) << 16)
                | ((buf[pos + len++] & 0xff) << 24);
        int n2 = (buf[pos + len++] & 0xff) | ((buf[pos + len++] & 0xff) << 8) | ((buf[pos + len++] & 0xff) << 16)
                | ((buf[pos + len++] & 0xff) << 24);
        pos += 8;
        return (((long) n1) & 0xffffffffL) | (((long) n2) << 32);
    }

    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    public boolean readBoolean() {
        ensureBounds(1);
        boolean res = false;
        if (buf[pos] == (byte) 1)
            res = true;
        pos++;
        return res;
    }

    public void readNull() {
    }

    public void ensureBounds(int num) {
        int remaining = limit - pos;
        try {
            if (remaining < num) {
                throw new EOFException();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

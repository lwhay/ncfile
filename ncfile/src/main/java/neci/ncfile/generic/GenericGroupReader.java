package neci.ncfile.generic;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import misc.GroupCore;
import neci.ncfile.base.NeciRuntimeException;
import neci.ncfile.base.Schema;
import neci.ncfile.base.Schema.Field;
import neci.ncfile.generic.GenericData.Group;
import neci.ncfile.generic.GenericData.Record;

public class GenericGroupReader {
    static GenericData data = GenericData.get();
    static byte[] buf;
    static int pos;
    static int limit;

    public static Group readGroup(GroupCore buffer, Schema schema) {
        Group res = new Group(schema);
        buf = buffer.array();
        pos = 0;
        limit = buf.length;
        int i = 0;
        for (Field f : schema.getFields()) {
            res.put(i++, readField(f));
        }
        return res;
    }

    static Record readRecord(Schema schema) {
        Record res = new Record(schema);
        int i = 0;
        for (Field f : schema.getFields()) {
            res.put(i++, readField(f));
        }
        return res;
    }

    static Object readField(Field f) {
        return read(f.schema());
    }

    static Object read(Schema schema) {
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

    static Object readArray(Schema schema) {
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

    static Object readUnion(Schema schema) {
        ensureBounds(1);
        int index = buf[pos++] & 0xff;
        return read(schema.getTypes().get(index));
    }

    static String readString() {
        int len = readInt();
        ensureBounds(len);
        String res = new String(buf, pos, len);
        pos += len;
        return res;
    }

    static ByteBuffer readBytes() {
        int len = readInt();
        ensureBounds(len);
        ByteBuffer res = ByteBuffer.allocate(len);
        System.arraycopy(buf, pos, res, 0, len);
        pos += len;
        return res;
    }

    static int readInt() {
        ensureBounds(4);
        int len = 1;
        int n = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8) | ((buf[pos + len++] & 0xff) << 16)
                | ((buf[pos + len++] & 0xff) << 24);
        pos += 4;
        return n;
    }

    static long readLong() {
        int len = 1;
        int n1 = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8) | ((buf[pos + len++] & 0xff) << 16)
                | ((buf[pos + len++] & 0xff) << 24);
        int n2 = (buf[pos + len++] & 0xff) | ((buf[pos + len++] & 0xff) << 8) | ((buf[pos + len++] & 0xff) << 16)
                | ((buf[pos + len++] & 0xff) << 24);
        pos += 8;
        return (((long) n1) & 0xffffffffL) | (((long) n2) << 32);
    }

    static float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    static double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    static boolean readBoolean() {
        ensureBounds(1);
        boolean res = false;
        if (buf[pos] == (byte) 1)
            res = true;
        pos++;
        return res;
    }

    static void readNull() {
    }

    static void ensureBounds(int num) {
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

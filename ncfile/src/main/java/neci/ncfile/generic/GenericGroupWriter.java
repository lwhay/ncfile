package neci.ncfile.generic;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import exceptions.NeciTypeException;
import misc.GroupCore;
import neci.ncfile.base.Schema;
import neci.ncfile.base.Schema.Field;

public class GenericGroupWriter {
    static GenericData data = GenericData.get();

    public static GroupCore writeGroup(Schema schema, Object datum) {
        GroupCore buf = new GroupCore();
        buf.allocate(8192);
        Object state = data.getRecordState(datum, schema);
        for (Field f : schema.getFields()) {
            writeField(datum, f, buf, state);
        }
        buf.trunk();
        return buf;
    }

    static void writeRecord(Schema schema, Object datum, GroupCore buf) {
        Object state = data.getRecordState(datum, schema);
        for (Field f : schema.getFields()) {
            writeField(datum, f, buf, state);
        }
    }

    static void writeField(Object datum, Field f, GroupCore buf, Object state) {
        Object value = data.getField(datum, f.name(), f.pos(), state);
        write(f.schema(), value, buf);
    }

    static void write(Schema schema, Object datum, GroupCore buf) {
        switch (schema.getType()) {
            case RECORD:
                writeRecord(schema, datum, buf);
                break;
            //            case ENUM:
            //                writeEnum(schema, datum, buf);
            //                break;
            case ARRAY:
                writeArray(schema, datum, buf);
                break;
            //            case MAP:
            //                writeMap(schema, datum, buf);
            //                break;
            case UNION:
                //                int index = resolveUnion(schema, datum);
                //                out.writeIndex(index);
                //                write(schema.getTypes().get(index), datum, buf);
                writeUnion(schema, datum, buf);
                break;
            //            case FIXED:
            //                writeFixed(schema, datum, buf);
            //                break;
            case STRING:
                writeString((String) datum, buf);
                break;
            case BYTES:
                writeBytes((ByteBuffer) datum, buf);
                break;
            case INT:
                writeInt(((Number) datum).intValue(), buf);
                break;
            case LONG:
                writeLong((Long) datum, buf);
                break;
            case FLOAT:
                writeFloat((Float) datum, buf);
                break;
            case DOUBLE:
                writeDouble((Double) datum, buf);
                break;
            case BOOLEAN:
                writeBoolean((Boolean) datum, buf);
                break;
            case NULL:
                writeNull();
                break;
            default:
                throw new NeciTypeException("Not a " + schema + ": " + datum);
        }
    }

    static void writeArray(Schema schema, Object datum, GroupCore buf) {
        Schema element = schema.getElementType();
        Collection<? extends Object> array = (Collection<? extends Object>) datum;
        int size = array.size();
        int actualSize = 0;
        writeInt(size, buf);
        for (Iterator<? extends Object> it = array.iterator(); it.hasNext();) {
            write(element, it.next(), buf);
            actualSize++;
        }
        if (actualSize != size) {
            throw new ConcurrentModificationException(
                    "Size of array written was " + size + ", but number of elements written was " + actualSize + ". ");
        }
    }

    static void writeUnion(Schema schema, Object datum, GroupCore buf) {
        int index = data.resolveUnion(schema, datum);
        writeByte((byte) index, buf);
        write(schema.getTypes().get(index), datum, buf);
    }

    static void writeString(String datum, GroupCore buf) {
        writeInt(datum.length(), buf);
        ensureBounds(datum.length(), buf);
        buf.put(datum.getBytes());
    }

    static void writeBytes(ByteBuffer datum, GroupCore buf) {
        ensureBounds(datum.limit(), buf);
        buf.put(datum);
    }

    static void writeInt(int n, GroupCore buf) {
        ensureBounds(4, buf);
        byte[] r = new byte[4];
        r[0] = (byte) ((n) & 0xFF);
        r[1] = (byte) ((n >>> 8) & 0xFF);
        r[2] = (byte) ((n >>> 16) & 0xFF);
        r[3] = (byte) ((n >>> 24) & 0xFF);
        buf.put(r);
    }

    static void writeLong(long n, GroupCore buf) {
        ensureBounds(8, buf);
        int first = (int) (n & 0xFFFFFFFF);
        int second = (int) ((n >>> 32) & 0xFFFFFFFF);
        byte[] r = new byte[8];
        r[0] = (byte) ((first) & 0xFF);
        r[4] = (byte) ((second) & 0xFF);
        r[5] = (byte) ((second >>> 8) & 0xFF);
        r[1] = (byte) ((first >>> 8) & 0xFF);
        r[2] = (byte) ((first >>> 16) & 0xFF);
        r[6] = (byte) ((second >>> 16) & 0xFF);
        r[7] = (byte) ((second >>> 24) & 0xFF);
        r[3] = (byte) ((first >>> 24) & 0xFF);
        buf.put(r);
    }

    static void writeFloat(float datum, GroupCore buf) {
        int bits = Float.floatToRawIntBits(datum);
        writeInt(bits, buf);
    }

    static void writeDouble(double datum, GroupCore buf) {
        long bits = Double.doubleToRawLongBits(datum);
        writeLong(bits, buf);
    }

    static void writeBoolean(boolean datum, GroupCore buf) {
        if (datum)
            writeByte((byte) 1, buf);
        else
            writeByte((byte) 0, buf);
    }

    static void writeByte(byte datum, GroupCore buf) {
        ensureBounds(1, buf);
        buf.put(datum);
    }

    static void writeNull() {
    }

    static void ensureBounds(int num, GroupCore buf) {
        if (buf.remaining() < num) {
            int x = num - buf.remaining();
            int exLen = 512;
            while (x > exLen) {
                exLen += 512;
            }
            GroupCore newBuf = new GroupCore();
            newBuf.allocate(buf.limit() + exLen);
            newBuf.put(buf.array());
            buf = newBuf;
        }
    }
}

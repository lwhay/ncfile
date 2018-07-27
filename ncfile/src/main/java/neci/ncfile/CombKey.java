package neci.ncfile;

import java.util.List;

import org.apache.trevni.ValueType;

import neci.ncfile.base.Schema.Field;
import neci.ncfile.generic.GenericData.Record;

public class CombKey implements Comparable<CombKey> {
    private long[] keys;

    public CombKey(int len) {
        keys = new long[len];
    }

    public CombKey(Record record) {
        List<Field> fs = record.getSchema().getFields();
        int len = fs.size();
        this.keys = new long[len];
        for (int i = 0; i < len; i++) {
            keys[i] = Integer.parseInt(record.get(i).toString());
        }
    }

    public CombKey(String k) {
        this(k.split("\\|"));
    }

    public CombKey(String[] k) {
        keys = new long[k.length];
        for (int i = 0; i < k.length; i++)
            keys[i] = Integer.parseInt(k[i]);
    }

    public CombKey(Record record, int len) {
        this.keys = new long[len];
        List<Field> fs = record.getSchema().getFields();
        for (int i = 0; i < len; i++) {
            keys[i] = Long.parseLong(record.get(i).toString());
        }
    }

    public CombKey(KeyofBTree o) {
        this.keys = o.values;
    }

    public CombKey(Record record, int[] keyFields) {
        int len = keyFields.length;
        this.keys = new long[len];
        List<Field> fs = record.getSchema().getFields();
        for (int i = 0; i < len; i++) {
            keys[i] = Long.parseLong(record.get(keyFields[i]).toString());
        }
    }

    public CombKey(long[] keys) {
        this.keys = keys;
    }

    public static boolean isInteger(Field f) {
        switch (f.schema().getType()) {
            case INT:
                return true;
            case LONG:
                return false;
            default:
                throw new ClassCastException("This type is not supported for Key type: " + f.schema());
        }
    }

    boolean isInteger(ValueType type) {
        switch (type) {
            case INT:
                return true;
            case LONG:
                return false;
            default:
                throw new ClassCastException("This type is not supported for Key type: " + type);
        }
    }

    @Override
    public int compareTo(CombKey o) {
        assert (this.getLength() == o.getLength());
        for (int i = 0; i < getLength(); i++) {
            if (keys[i] > o.keys[i])
                return 1;
            else if (keys[i] < o.keys[i])
                return -1;
        }
        return 0;
    }

    public int compareTo(KeyofBTree o) {
        assert (this.getLength() == o.getLength());
        for (int i = 0; i < getLength(); i++) {
            if (keys[i] > o.values[i])
                return 1;
            else if (keys[i] < o.values[i])
                return -1;
        }
        return 0;
    }

    @Override
    public String toString() {
        String res = "";
        for (int i = 0; i < keys.length - 1; i++)
            res += keys[i] + "|";
        res += keys[keys.length - 1];
        return res;
    }

    @Override
    public int hashCode() {
        return (int) keys[0];
    }

    @Override
    public boolean equals(Object o) {
        return (compareTo((CombKey) o) == 0);
    }

    public int getLength() {
        return keys.length;
    }

    public long[] get() {
        return keys;
    }

    public Object get(int i) {
        return keys[i];
    }

    public CombKey get(int[] fields) {
        long[] k = new long[fields.length];
        for (int i = 0; i < fields.length; i++) {
            k[i] = keys[fields[i]];
        }
        return new CombKey(k);
    }
}

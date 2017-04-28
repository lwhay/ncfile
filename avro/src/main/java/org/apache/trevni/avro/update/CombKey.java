package org.apache.trevni.avro.update;

import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.ValueType;

public class CombKey implements Comparable<CombKey> {
    private Object[] keys;
    private boolean[] types;

    public CombKey(int len) {
        keys = new Object[len];
        types = new boolean[len];
    }

    public CombKey(Record record) {
        List<Field> fs = record.getSchema().getFields();
        int len = fs.size();
        this.keys = new Object[len];
        this.types = new boolean[len];
        for (int i = 0; i < len; i++) {
            types[i] = isInteger(fs.get(i));
            keys[i] = record.get(i);
        }
    }

    public CombKey(Record record, int len) {
        this.keys = new Object[len];
        this.types = new boolean[len];
        List<Field> fs = record.getSchema().getFields();
        for (int i = 0; i < len; i++) {
            types[i] = isInteger(fs.get(i));
            keys[i] = record.get(i);
        }
    }

    public CombKey(KeyofBTree o) {
        this.keys = o.values;
        this.types = o.types;
    }

    public CombKey(Record record, int[] keyFields) {
        int len = keyFields.length;
        this.keys = new Object[len];
        this.types = new boolean[len];
        List<Field> fs = record.getSchema().getFields();
        for (int i = 0; i < len; i++) {
            types[i] = isInteger(fs.get(keyFields[i]));
            keys[i] = record.get(keyFields[i]);
        }
    }

    public CombKey(Object[] keys, ValueType[] tt) {
        assert (keys.length == tt.length);
        this.keys = keys;
        this.types = new boolean[keys.length];
        for (int i = 0; i < keys.length; i++) {
            types[i] = isInteger(tt[i]);
        }
    }

    boolean isInteger(Field f) {
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

    public CombKey(Object[] keys, boolean[] types) {
        assert (keys.length == types.length);
        this.keys = keys;
        this.types = types;
    }

    @Override
    public int compareTo(CombKey o) {
        assert (this.getLength() == o.getLength());
        assert (types == o.types);
        for (int i = 0; i < getLength(); i++) {
            if (types[i]) {
                long k1 = Long.parseLong(keys[i].toString());
                long k2 = Long.parseLong(o.keys[i].toString());
                if (k1 > k2)
                    return 1;
                else if (k1 < k2)
                    return -1;
            } else {
                String k1 = keys[i].toString();
                String k2 = o.keys[i].toString();
                if (k1.compareTo(k2) > 0)
                    return 1;
                else if (k1.compareTo(k2) < 0)
                    return -1;
            }
        }
        return 0;
    }

    @Override
    public int hashCode() {
        return keys[0].hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return (compareTo((CombKey) o) == 0);
    }

    public int getLength() {
        return keys.length;
    }

    public Object[] get() {
        return keys;
    }

    public Object get(int i) {
        return keys[i];
    }

    public CombKey get(int[] fields) {
        Object[] k = new Object[fields.length];
        boolean[] t = new boolean[fields.length];
        for (int i = 0; i < fields.length; i++) {
            k[i] = keys[fields[i]];
            t[i] = types[fields[i]];
        }
        return new CombKey(k, t);
    }

    public boolean getType(int i) {
        return types[i];
    }

    public boolean[] getTypes() {
        return types;
    }
}

package org.apache.trevni.avro.update;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;

import btree.Serializable;
import btree.Utils;

public class KeyofBTree implements Comparable<KeyofBTree>, Serializable {
    int[] values;

    public KeyofBTree() {
    }

    public KeyofBTree(CombKey key) {
        values = key.get();
    }

    public KeyofBTree(Record record) {
        List<Field> fs = record.getSchema().getFields();
        int len = fs.size();
        this.values = new int[len];
        for (int i = 0; i < len; i++) {
            values[i] = Integer.parseInt(record.get(i).toString());
        }
    }

    public KeyofBTree(String k) {
        this(k.split("\\|"));
    }

    public KeyofBTree(String[] keys) {
        values = new int[keys.length];
        for (int i = 0; i < keys.length; i++)
            values[i] = Integer.parseInt(keys[i]);
    }

    public KeyofBTree(int[] keys) {
        values = keys;
    }

    public KeyofBTree(String[] keys, int len) {
        values = new int[len];
        for (int i = 0; i < len; i++)
            values[i] = Integer.parseInt(keys[i]);
    }

    public KeyofBTree(Record record, int len) {
        this.values = new int[len];
        List<Field> fs = record.getSchema().getFields();
        for (int i = 0; i < len; i++) {
            values[i] = Integer.parseInt(record.get(i).toString());
        }
    }

    public KeyofBTree(Record record, int[] keyFields) {
        int len = keyFields.length;
        this.values = new int[len];
        List<Field> fs = record.getSchema().getFields();
        for (int i = 0; i < len; i++) {
            values[i] = Integer.parseInt(record.get(keyFields[i]).toString());
        }
    }

    public KeyofBTree get(int[] fields) {
        int[] k = new int[fields.length];
        for (int i = 0; i < fields.length; i++) {
            k[i] = values[fields[i]];
        }
        return new KeyofBTree(k);
    }

    public KeyofBTree(byte[] data) {
        deseriablize(data);
    }

    public int[] getKey() {
        return values;
    }

    public int getLength() {
        return values.length;
    }

    public long getBytesSize() {
        return 4 * getLength();
    }

    public byte[] getBytes4(int data) {
        byte[] res = new byte[4];
        res[0] = (byte) (data & 0xff);
        res[1] = (byte) ((data >> 8) & 0xff);
        res[2] = (byte) ((data >> 16) & 0xff);
        res[3] = (byte) ((data >> 24) & 0xff);
        return res;
    }

    public byte[] getBytes8(long data) {
        byte[] res = new byte[8];
        res[0] = (byte) (data & 0xff);
        res[1] = (byte) ((data >> 8) & 0xff);
        res[2] = (byte) ((data >> 16) & 0xff);
        res[3] = (byte) ((data >> 24) & 0xff);
        res[4] = (byte) ((data >> 32) & 0xff);
        res[5] = (byte) ((data >> 40) & 0xff);
        res[6] = (byte) ((data >> 48) & 0xff);
        res[7] = (byte) ((data >> 56) & 0xff);
        return res;
    }

    @Override
    public byte[] serialize() {
        List<Byte> res = new ArrayList<Byte>();
        int i = 0;
        for (int v : values) {
            for (byte b : getBytes4(v))
                res.add(b);
        }
        byte[] ee = new byte[res.size()];
        for (i = 0; i < ee.length; i++) {
            ee[i] = res.get(i);
        }
        return ee;
    }

    @Override
    public void deseriablize(byte[] data) {
        int index = 0;
        int len = data.length / 4;
        values = new int[len];
        int in = 0;
        while (in < len) {
            values[in] = Utils.getInt(data, index);
            in++;
            index += 4;
        }
    }

    @Override
    public String toString() {
        String res = "";
        for (int i = 0; i < values.length - 1; i++)
            res += values[i] + "|";
        res += values[values.length - 1];
        return res;
    }

    @Override
    public int hashCode() {
        return values[0];
    }

    @Override
    public int compareTo(KeyofBTree o) {
        int len = values.length;
        for (int i = 0; i < len; i++) {
            if (values[i] > o.values[i])
                return 1;
            if (values[i] < o.values[i])
                return -1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        return (compareTo((KeyofBTree) o) == 0);
    }
}

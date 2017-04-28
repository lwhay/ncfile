package org.apache.trevni.avro.update;

import java.util.ArrayList;
import java.util.List;

import btree.Serializable;
import btree.Utils;

public class KeyofBTree implements Comparable<KeyofBTree>, Serializable {
    Object[] values;
    boolean[] types;

    public KeyofBTree() {

    }

    public KeyofBTree(CombKey key) {
        values = key.get();
        types = key.getTypes();
    }

    public KeyofBTree(byte[] data) {
        deseriablize(data);
    }

    public Object[] getKey() {
        return values;
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
        for (Object v : values) {
            if (types[i]) {
                res.add((byte) 0);
                for (byte b : getBytes4(Integer.parseInt(v.toString())))
                    res.add(b);
            } else {
                res.add((byte) 1);
                for (byte b : getBytes8(Long.parseLong(v.toString())))
                    res.add(b);
            }
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
        int len = data.length;
        List<Object> vs = new ArrayList<Object>();
        String tt = "";
        int i = 0;
        while (index < len) {
            byte t = data[index++];
            tt += t;
            if (t == (byte) 0) {
                vs.add(Utils.getInt(data, index));
                index += 4;
            } else {
                vs.add(Utils.getLong(data, index));
                index += 8;
            }
        }
        types = new boolean[tt.length()];
        i = 0;
        for (byte x : tt.getBytes()) {
            if (x == (byte) '0') {
                types[i++] = true;
            } else {
                types[i++] = false;
            }
        }
        values = vs.toArray();
    }

    @Override
    public int hashCode() {
        return (int) values[0];
    }

    @Override
    public int compareTo(KeyofBTree o) {
        int len = values.length;
        for (int i = 0; i < len; i++) {
            int re;
            if (types[i]) {
                re = ((int) values[i] > (int) o.values[i]) ? 1 : (((int) values[i] < (int) o.values[i]) ? -1 : 0);
            } else {
                re = ((long) values[i] > (int) o.values[i]) ? 1 : (((int) values[i] < (long) o.values[i]) ? -1 : 0);
            }
            //            int re = (Long.parseLong(values[i].toString()) > Long.parseLong(o.values[i].toString())) ? 1
            //                    : ((Long.parseLong(values[i].toString()) < Long.parseLong(o.values[i].toString())) ? -1 : 0);
            if (re != 0)
                return re;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        return (compareTo((KeyofBTree) o) == 0);
    }
}

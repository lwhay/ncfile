package org.apache.trevni.avro.update;

public class NestCombKey implements Comparable<NestCombKey> {
    CombKey[] keys;
    int le;

    public NestCombKey(CombKey[] keys) {
        this.keys = keys;
        le = keys.length;
    }

    public NestCombKey(CombKey key) {
        this.keys = new CombKey[] { key };
        le = 1;
    }

    public NestCombKey(NestCombKey key1, CombKey key2) {
        keys = new CombKey[key1.le + 1];
        int i = 0;
        for (CombKey kk : key1.keys) {
            keys[i++] = kk;
        }
        keys[i] = key2;
        le = keys.length;
    }

    public NestCombKey(NestCombKey key1, NestCombKey key2) {
        keys = new CombKey[key1.le + key2.le];
        int i = 0;
        for (CombKey kk : key1.keys) {
            keys[i++] = kk;
        }
        for (CombKey kk : key2.keys) {
            keys[i++] = kk;
        }
        le = keys.length;
    }

    @Override
    public int compareTo(NestCombKey o) {
        assert (this.le == o.le);
        int re;
        for (int i = 0; i < le; i++) {
            re = keys[i].compareTo(o.getKey(i));
            if (re != 0) {
                return re;
            }
        }
        return 0;
    }

    public int compareTo(CombKey o, int l) {
        return keys[l].compareTo(o);
    }

    public int compareTo(NestCombKey o, int le) {
        assert (this.le >= le);
        int re;
        for (int i = 0; i < le; i++) {
            re = keys[i].compareTo(o.getKey(i));
            if (re != 0) {
                return re;
            }
        }
        return 0;
    }

    public CombKey getKey(int i) {
        assert (i < le);
        return keys[i];
    }

    @Override
    public int hashCode() {
        return keys[0].hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return (compareTo((NestCombKey) o) == 0);
    }
}

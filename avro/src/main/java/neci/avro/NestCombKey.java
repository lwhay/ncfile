package neci.avro;

public class NestCombKey implements Comparable<NestCombKey> {
    KeyofBTree[] keys;
    int le;

    public NestCombKey(KeyofBTree[] keys) {
        this.keys = keys;
        le = keys.length;
    }

    public NestCombKey(KeyofBTree key) {
        this.keys = new KeyofBTree[] { key };
        le = 1;
    }

    public NestCombKey(NestCombKey key1, KeyofBTree key2) {
        keys = new KeyofBTree[key1.le + 1];
        int i = 0;
        for (KeyofBTree kk : key1.keys) {
            keys[i++] = kk;
        }
        keys[i] = key2;
        le = keys.length;
    }

    public NestCombKey(NestCombKey key1, NestCombKey key2) {
        keys = new KeyofBTree[key1.le + key2.le];
        int i = 0;
        for (KeyofBTree kk : key1.keys) {
            keys[i++] = kk;
        }
        for (KeyofBTree kk : key2.keys) {
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

    public int compareTo(KeyofBTree o, int l) {
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

    public KeyofBTree getKey(int i) {
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

package misc;

public class KeyGroup implements Comparable<KeyGroup> {
    private byte flag;
    private int[] keys;

    public byte getFlag() {
        return flag;
    }

    public void setFlag(byte flag) {
        this.flag = flag;
    }

    public int[] getKeys() {
        return keys;
    }

    public void setKeys(int[] keys) {
        this.keys = keys;
    }

    public KeyGroup(byte flag, int[] keys) {
        this.flag = flag;
        this.keys = keys;
    }

    @Override
    public int compareTo(KeyGroup o) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] > o.keys[i])
                return 1;
            else if (keys[i] < o.keys[i])
                return -1;
        }
        return 0;
    }
}

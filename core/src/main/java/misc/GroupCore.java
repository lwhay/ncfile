package misc;

import java.nio.ByteBuffer;

public class GroupCore implements Comparable<GroupCore> {
    private ByteBuffer value;

    public GroupCore() {

    }

    public GroupCore(ByteBuffer buf) {
        value = ByteBuffer.wrap(buf.array());
    }

    public void allocate(int capacity) {
        value = ByteBuffer.allocate(capacity);
    }

    public void put(ByteBuffer buf) {
        value.put(buf);
    }

    public void put(byte[] buf) {
        value.put(buf);
    }

    public void put(byte b) {
        value.put(b);
    }

    public byte get(int i) {
        return value.get(i);
    }

    public int position() {
        return value.position();
    }

    public int remaining() {
        return value.remaining();
    }

    public int limit() {
        return value.limit();
    }

    public void trunk() {
        int pos = value.position();
        ByteBuffer tvalue = ByteBuffer.allocate(pos);
        System.arraycopy(value.array(), 0, tvalue.array(), 0, pos);
        value = tvalue;
    }

    public byte[] array() {
        return value.array();
    }

    @Override
    public int compareTo(GroupCore that) {
        int n = this.position() + Math.min(this.remaining(), that.remaining());
        for (int i = this.position(), j = that.position(); i < n; i++, j++) {
            int cmp = Byte.compare(this.get(i), that.get(j));
            if (cmp != 0)
                return cmp;
        }
        return this.remaining() - that.remaining();
    }
}

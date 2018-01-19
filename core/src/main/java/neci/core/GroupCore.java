package neci.core;

import java.nio.ByteBuffer;

public class GroupCore {
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

    public int remaining() {
        return value.remaining();
    }

    public int limit() {
        return value.limit();
    }

    public byte[] array() {
        return value.array();
    }
}

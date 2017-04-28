package btree;

/*
 * Convert stanford DB data to a user-defined type for Btree
 */
public class Info implements Serializable {
    public String content;
    public String type;

    public Info(String type, String content) {
        this.type = type;
        this.content = content;
    }

    public Info() {

    }

    @Override
    public byte[] serialize() {
        // TODO Auto-generated method stub
        byte[] result = new byte[type.length() + content.length()];
        System.arraycopy(type.getBytes(), 0, result, 0, 1);
        System.arraycopy(content.getBytes(), 0, result, 1, content.length());
        return result;
    }

    @Override
    public void deseriablize(byte[] data) {
        // TODO Auto-generated method stub
        this.type = new String(data, 0, 1);
        this.content = new String(data, 1, data.length - 1);
    }

    @Override
    public String toString() {
        return this.type + this.content;
    }
}

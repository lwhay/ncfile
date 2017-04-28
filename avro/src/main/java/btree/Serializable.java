package btree;

/*
 user-definited type for btree should implements this interface
 */
public interface Serializable {

    public abstract byte[] serialize();// convert your object to byte data

    public abstract void deseriablize(byte[] data);// recover your object from byte data
}

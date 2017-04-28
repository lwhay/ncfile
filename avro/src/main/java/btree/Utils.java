package btree;

public class Utils {
    public static int getInt(byte[] bytes, int begin) {
        return (0xff & bytes[begin]) | (0xff00 & (bytes[begin + 1] << 8)) | (0xff0000 & (bytes[begin + 2] << 16))
                | (0xff000000 & (bytes[begin + 3] << 24));
    }

    public static double getDouble(byte[] data, int begin) {
        long value = getLong(data, begin);
        return Double.longBitsToDouble(value);
    }

    public static void getDoubleBytes(Double data, byte[] dest, int off) {
        long value = Double.doubleToLongBits(data);
        getBytes8(value, dest, off);
    }

    public static short getShort(byte[] bytes, int begin) {
        return (short) ((0xff & bytes[begin]) | (0xff00 & (bytes[begin + 1] << 8)));
    }

    public static long getLong(byte[] bytes, int begin) {
        return (0xffL & (long) bytes[begin]) | (0xff00L & ((long) bytes[begin + 1] << 8))
                | (0xff0000L & ((long) bytes[begin + 2] << 16)) | (0xff000000L & ((long) bytes[begin + 3] << 24))
                | (0xff00000000L & ((long) bytes[begin + 4] << 32))
                | (0xff0000000000L & ((long) bytes[begin + 5] << 40))
                | (0xff000000000000L & ((long) bytes[begin + 6] << 48))
                | (0xff00000000000000L & ((long) bytes[begin + 7] << 56));
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<T>, V> int bSearch(Object[] arr, int begin, int end, T key) {
        int mid, left = begin, right = end - 1;
        while (left <= right) {
            mid = (left + right) >> 1;
            if (key.compareTo((T) arr[mid]) <= 0)
                right = mid - 1;
            else
                left = mid + 1;
        }
        return left;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<T>, V> int bSearch2(Object[] arr, int begin, int end, T key) {
        int mid, left = begin, right = end - 1;
        while (left <= right) {
            mid = (left + right) >> 1;
            if (key.compareTo((T) arr[mid]) < 0)
                right = mid - 1;
            else
                left = mid + 1;
        }
        return left;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Comparable<T>, V> int bSearch3(Object[] arr, int begin, int end, T key) {
        int mid, left = begin, right = end - 1;
        while (left <= right) {
            mid = (left + right) >> 1;
            if (key.compareTo((T) arr[mid]) < 0)
                right = mid - 1;
            else
                left = mid + 1;
        }
        return right;
    }

    public static void getBytes8(long data, byte[] dest, int off) {
        dest[off] = (byte) (data & 0xff);
        dest[off + 1] = (byte) ((data >> 8) & 0xff);
        dest[off + 2] = (byte) ((data >> 16) & 0xff);
        dest[off + 3] = (byte) ((data >> 24) & 0xff);
        dest[off + 4] = (byte) ((data >> 32) & 0xff);
        dest[off + 5] = (byte) ((data >> 40) & 0xff);
        dest[off + 6] = (byte) ((data >> 48) & 0xff);
        dest[off + 7] = (byte) ((data >> 56) & 0xff);
    }

    public static void getBytes4(int data, byte[] dest, int off) {
        dest[off] = (byte) (data & 0xff);
        dest[off + 1] = (byte) ((data & 0xff00) >> 8);
        dest[off + 2] = (byte) ((data & 0xff0000) >> 16);
        dest[off + 3] = (byte) ((data & 0xff000000) >> 24);
    }

    public static void getBytes2(short data, byte[] dest, int off) {
        dest[off] = (byte) (data & 0xff);
        dest[off + 1] = (byte) ((data & 0xff00) >> 8);
    }

    public static long getAddress(byte[] add) {
        return (long) getInt(add, 2) * getShort(add, 0);
    }
}

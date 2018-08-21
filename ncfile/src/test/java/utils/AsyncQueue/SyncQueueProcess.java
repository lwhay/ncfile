/**
 * 
 */
package utils.AsyncQueue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Michael
 *
 */
public class SyncQueueProcess {
    private static int buflen;

    private static long offset;

    private static ByteBuffer target;

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        buflen = Integer.parseInt(args[0]);
        FileInputStream reader = new FileInputStream(new File(args[3]));
        FileChannel chan = reader.getChannel();
        for (int i = 0; i < Integer.parseInt(args[1]) * Integer.parseInt(args[2]); i++) {
            target = ByteBuffer.allocate(buflen);
            chan.read(target, offset);
            offset += buflen;
        }
        reader.close();
    }
}

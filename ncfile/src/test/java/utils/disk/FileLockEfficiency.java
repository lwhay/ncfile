/**
 * 
 */
package utils.disk;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * @author Michael
 *
 */
public class FileLockEfficiency {

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile("swap.mm", "rw");
        FileChannel chan = raf.getChannel();
        //MappedByteBuffer mbb = chan.map(MapMode.READ_WRITE, 0, 1024);
        FileLock lock = null;
        long begin = System.nanoTime();
        int success = 0;
        for (int i = 0; i < 10; i++) {
            lock = chan.lock();
            if (lock != null) {
                success++;
                Thread.sleep(1000);
                lock.release();
            }
        }
        System.out.println("Success: " + success + " time: " + (System.nanoTime() - begin) / 1000000);
    }

}

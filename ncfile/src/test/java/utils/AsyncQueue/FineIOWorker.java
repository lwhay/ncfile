/**
 * 
 */
package utils.AsyncQueue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Michael
 *
 */
public class FineIOWorker implements Runnable {
    private static final boolean PACKING = new File("pack.mark").exists();
    private final int buflen;
    private final Integer Tick;
    private final Integer Repeat;
    private final Integer Volume;
    private int round;
    private BlockInputBufferQueue queue;
    private int count = 0;
    /*private Random random = new Random(47);*/
    private final FileChannel chan;
    private long position = 0;
    private int total = 0;
    private boolean terminate = false;
    public boolean completed = true;
    public boolean restarted = false;

    public FineIOWorker(BlockInputBufferQueue queue, Integer tick, Integer volume, Integer size, String path)
            throws FileNotFoundException {
        this.queue = queue;
        Tick = tick;
        round = tick;
        count = 0;
        Volume = volume;
        Repeat = size;
        buflen = size;
        completed = false;
        restarted = true;
        chan = new FileInputStream(new File(path)).getChannel();
    }

    public boolean full() {
        return count >= round;
    }

    public void reset(int round) {
        position = 0;
        count = 0;
        this.round = round;
        this.completed = false;
        this.restarted = true;
    }

    public void teriminate() {
        terminate = true;
        count = 0;
    }

    public int getCount() {
        return count;
    }

    public int getRound() {
        return round;
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                //System.out.println("count: " + count + " round: " + round);
                if (completed) {
                    /*System.out.println("Reset producer: " + position + " count: " + count + " round: " + round
                            + " total: " + total);*/
                    synchronized (Repeat) {
                        //completed = true;
                        Repeat.notify();
                    }
                    while (!restarted) {
                        synchronized (Tick) {
                            Tick.wait();
                        }
                    }
                    if (terminate) {
                        System.out.println("Child exit: " + total);
                        chan.close();
                        System.exit(0);
                    }
                    restarted = false;
                } else {
                    if (queue.size() < 5) {
                        int range = Math.min(round - count, Volume - queue.size());
                        /*System.out.println("    Producer: " + position + " count: " + count + " round: " + round
                                + " read: " + range + " size: " + queue.size());*/
                        if (PACKING) {
                            ByteBuffer buf = ByteBuffer.allocate(buflen * range);
                            int len = chan.read(buf, position);
                            if (len != buf.limit()) {
                                System.out.println("File too short");
                            }
                            for (int j = 0; j < range; j++) {
                                queue.put(new PositionalBlock<Long, byte[]>(position + j * buflen,
                                        ByteBuffer.wrap(buf.array(), j * buflen, buflen).array()));
                                /*System.out.println("\tProducer: " + position + " count: " + count + " round: " + round
                                + " size: " + queue.size());*/
                                count++;
                                total++;
                                position += buflen;
                                if (count == round) {
                                    completed = true;
                                    break;
                                }
                            }
                        } else {
                            for (int i = 0; i < range; i++) {
                                ByteBuffer buf = ByteBuffer.allocate(buflen);
                                int len = chan.read(buf, position);
                                if (len != buf.limit()) {
                                    System.out.println("File too short");
                                }
                                queue.put(new PositionalBlock<Long, byte[]>(position, buf.array()));
                                /*System.out.println("\tProducer: " + position + " count: " + count + " round: " + round
                                    + " size: " + queue.size());*/
                                count++;
                                total++;
                                position += buflen;
                                if (count == round) {
                                    completed = true;
                                    break;
                                }
                            }
                        }
                    }
                    //Thread.sleep(50);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}

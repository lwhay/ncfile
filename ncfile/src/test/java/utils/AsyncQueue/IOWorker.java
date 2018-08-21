/**
 * 
 */
package utils.AsyncQueue;

/**
 * @author Michael
 * @deprecated This Worker is wrong.
 *
 */
public class IOWorker implements Runnable {
    private final int buflen;
    private final Integer Mark;
    private final Integer Repeat;
    private int round;
    private BlockInputBufferQueue queue;
    private int count = 0;
    /*private Random random = new Random(47);*/
    private long position = 0;
    private int total = 0;
    private boolean terminate = false;
    public boolean completed = true;

    public IOWorker(BlockInputBufferQueue queue, Integer mark, Integer repeat) {
        this.queue = queue;
        Mark = mark;
        round = 0;
        count = repeat;
        Repeat = repeat;
        buflen = repeat;
        completed = false;
    }

    public boolean full() {
        return count >= round;
    }

    public void reset(int round) {
        position = 0;
        count = 0;
        this.round = round;
        this.completed = false;
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
                if (count == round) {
                    /*System.out.println("Reset producer: " + position + " count: " + count + " round: " + round
                            + " total: " + total);*/
                    synchronized (Repeat) {
                        completed = true;
                        Repeat.notify();
                    }
                    while (count != 0) {
                        synchronized (Mark) {
                            Mark.wait();
                            if (terminate) {
                                System.out.println("Child exit: " + total);
                                System.exit(0);
                            }
                        }
                    }
                } else {
                    if (queue.size() < 5) {
                        int range = Math.min(round - count, Mark - queue.size());
                        /*System.out.println("    Producer: " + position + " count: " + count + " round: " + round
                                + " read: " + range + " size: " + queue.size());*/
                        for (int i = 0; i < range; i++) {
                            queue.put(new PositionalBlock<Long, byte[]>(position, new byte[buflen]));
                            /*System.out.println("\tProducer: " + position + " count: " + count + " round: " + round
                                    + " size: " + queue.size());*/
                            count++;
                            total++;
                            position += buflen;
                        }
                    }
                    //Thread.sleep(50);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

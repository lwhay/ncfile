/**
 * 
 */
package utils.AsyncQueue;

/**
 * @author Michael
 *
 */
public class IteratorWorker implements Runnable {
    private BlockInputBufferQueue queue;

    private long position = 0;

    public IteratorWorker(BlockInputBufferQueue queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            PositionalBlock<Long, byte[]> payload;
            try {
                payload = queue.take();
                position += payload.getValue().length;
                /*System.out.println("\tConsumer: " + payload.getKey() + ":" + payload.getValue().length + "->" + position
                        + " size: " + queue.size());*/
                //Thread.sleep(80);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}

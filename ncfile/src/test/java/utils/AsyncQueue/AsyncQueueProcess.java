package utils.AsyncQueue;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncQueueProcess {
    private static final boolean useCoase = new File("coase.mark").exists();

    private static void coase(String[] args) throws InterruptedException {
        Integer Size = Integer.parseInt(args[0]);
        Integer Mark = Integer.parseInt(args[1]);
        Integer Tick = Integer.parseInt(args[2]);
        Integer Full = Integer.parseInt(args[3]);
        BlockInputBufferQueue queue = new BlockInputBufferQueue(Mark);
        ExecutorService service = Executors.newCachedThreadPool();
        IOWorker ioWorker = new IOWorker(queue, Mark, Size);
        service.execute(ioWorker);
        Runnable iterator = new IteratorWorker(queue);
        service.execute(iterator);
        int tick = 0;
        while (true) {
            if (ioWorker.full()) {
                if (tick++ >= Full) {
                    ioWorker.reset(0);
                    ioWorker.teriminate();
                    synchronized (Mark) {
                        Mark.notify();
                        System.out.println("Shutdown");
                    }
                    /*while (service.isTerminated()) {
                        Thread.sleep(50);
                    }*/
                    break;
                }
                //System.out.println(">\t" + tick + " count: " + ioWorker.getCount() + " round: " + ioWorker.getRound());
                ioWorker.reset(Tick);
                //System.out.println("<\t" + tick + " count: " + ioWorker.getCount() + " round: " + ioWorker.getRound());
                synchronized (Mark) {
                    Mark.notify();
                }
            } else {
                //System.out.println("Enter repeat");
                //Thread.sleep(20);
                synchronized (Size) {
                    try {
                        Size.wait();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                //System.out.println("Leave repeat");
            }
        }
        service.shutdown();
    }

    private static void fine(String[] args) throws InterruptedException, FileNotFoundException {
        Integer Size = Integer.parseInt(args[0]);
        Integer Volume = Integer.parseInt(args[1]);
        Integer Tick = Integer.parseInt(args[2]);
        Integer Full = Integer.parseInt(args[3]);
        BlockInputBufferQueue queue = new BlockInputBufferQueue(Volume);
        ExecutorService service = Executors.newCachedThreadPool();
        FineIOWorker ioWorker = new FineIOWorker(queue, Tick, Volume, Size, args[4]);
        service.execute(ioWorker);
        Runnable iterator = new IteratorWorker(queue);
        service.execute(iterator);
        int tick = 0;
        while (true) {
            //System.out.println("Enter repeat");
            while (!ioWorker.completed) {
                //System.out.println("Enter parent sync");
                synchronized (Size) {
                    Size.wait();
                }
                //System.out.println("Leave repeat");
            }
            if (++tick >= Full) {
                ioWorker.teriminate();
                ioWorker.reset(0);
                synchronized (Tick) {
                    Tick.notify();
                    System.out.println("Shutdown");
                }
                //Thread.sleep(50);
                break;
            }
            //System.out.println(">\t" + tick + " count: " + ioWorker.getCount() + " round: " + ioWorker.getRound());
            ioWorker.reset(Tick);
            //System.out.println("<\t" + tick + " count: " + ioWorker.getCount() + " round: " + ioWorker.getRound());
            synchronized (Tick) {
                Tick.notify();
            }
        }
        service.shutdown();
    }

    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        if (useCoase) {
            System.out.println("coase");
            coase(args);
        } else {
            System.out.println("fine");
            fine(args);
        }
    }
}

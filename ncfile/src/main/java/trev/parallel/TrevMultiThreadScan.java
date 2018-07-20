/**
 * 
 */
package trev.parallel;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;

import trev.parallel.worker.TrevScanner;

/**
 * @author Michael
 *
 */
public class TrevMultiThreadScan<T extends TrevScanner> {
    private static final int DEFAULT_READ_SCALE = 1;

    private final Class<T> scannerClass;

    private final Schema schema;

    private final String targetPath;

    private final int degree;

    private final int batchSize;

    private final Thread[] threads;

    private final Runnable[] workers;

    public TrevMultiThreadScan(final Class<T> scannerClass, String schemaPath, String targetPath, int degree, int bs)
            throws IOException {
        this.schema = (new Schema.Parser()).parse(new File(schemaPath));
        this.scannerClass = scannerClass;
        this.targetPath = targetPath;
        this.degree = degree;
        this.batchSize = bs;
        this.threads = new Thread[degree];
        this.workers = new Runnable[degree];
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public void scan() throws IOException, InterruptedException, InstantiationException, IllegalAccessException {
        for (int i = 0; i < degree; i++) {
            String path = targetPath + i + "/result.trev";
            if (!new File(path).exists()) {
                continue;
            }
            workers[i] = new TrevScanThreadFactory<T>(scannerClass, schema, path, batchSize * DEFAULT_READ_SCALE).create();
            threads[i] = new Thread(workers[i]);
            threads[i].start();
        }
        /*boolean finished = false;
        int count = 0;
        while (!finished) {
            finished = true;
            for (int i = 0; i < degree; i++) {
                if (threads[i].isAlive()) {
                    ScanThread worker = (ScanThread) workers[i];
                    worker.lock();
                    int lc = worker.fetch().size();
                    worker.reset();
                    worker.release();
                    worker.unlock();
                    System.out.println("Worker " + i + " fetched: " + lc);
                    count += lc;
                    finished = false;
                } else {
                    //threads[i].join();
                    finished |= true;
                }
            }
        }
        System.out.println("Total count: " + count);*/
        for (int i = 0; i < degree; i++) {
            threads[i].join();
        }
    }
}

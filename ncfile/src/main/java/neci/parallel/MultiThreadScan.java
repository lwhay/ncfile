/**
 * 
 */
package neci.parallel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import neci.ncfile.base.Schema;
import neci.parallel.worker.Scanner;

/**
 * @author Michael
 *
 */
public class MultiThreadScan<T extends Scanner> {
    protected static final int DEFAULT_READ_SCALE = 1;

    protected final Class<T> scannerClass;

    protected final Schema schema;

    protected final String targetPath;

    protected final int degree;

    protected final int batchSize;

    protected final Thread[] threads;

    protected final List<T> workers;

    public MultiThreadScan(final Class<T> scannerClass, String schemaPath, String targetPath, int degree, int bs)
            throws IOException {
        this.schema = (new Schema.Parser()).parse(new File(schemaPath));
        this.scannerClass = scannerClass;
        this.targetPath = targetPath;
        this.degree = degree;
        this.batchSize = bs;
        this.threads = new Thread[degree];
        this.workers = new ArrayList<>();
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
            String path = targetPath + i + "/result.neci";
            if (!new File(path).exists()) {
                continue;
            }
            workers.add(new ScanThreadFactory<T>(scannerClass, schema, path, batchSize * DEFAULT_READ_SCALE).create());
            threads[i] = new Thread(workers.get(i));
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

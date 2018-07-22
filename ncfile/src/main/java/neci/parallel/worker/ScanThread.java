package neci.parallel.worker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import neci.ncfile.BatchColumnReader;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

public class ScanThread extends Scanner {

    protected Schema schema;

    protected BatchColumnReader<Record> reader;

    protected String path;

    protected int batchSize;

    protected List<Record> listRecord = new ArrayList<>();

    private ReadWriteLock lock;

    private Semaphore fetched = new Semaphore(0);

    public void init(Schema schema, String path, int batchSize) {
        this.schema = schema;
        this.path = path;
        this.batchSize = batchSize;
        this.lock = new ReentrantReadWriteLock();
        //this.lock();
        //System.out.println("Thread created: bs " + batchSize);
    }

    public void acquire() {
        try {
            fetched.acquire();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void release() {
        fetched.release();
    }

    public void lock() {
        System.out.println("enter");
        this.lock.writeLock().lock();
    }

    public void unlock() {
        System.out.println("leave");
        this.lock.writeLock().unlock();
    }

    public List<Record> fetch() {
        System.out.println("\tfetch");
        return listRecord;
    }

    public void reset() {
        listRecord = new ArrayList<>();
    }

    @Override
    public void run() {
        String outline = "";
        System.out.println(path);
        outline = "\tBegin path: " + path + " ";
        long begin = System.currentTimeMillis();
        if (reader == null) {
            try {
                reader = new BatchColumnReader<>(new File(path));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            reader.createSchema(schema);
            try {
                reader.create();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        outline += " prepared: " + (System.currentTimeMillis() - begin) + " ";
        begin = System.currentTimeMillis();
        int count = 0;
        while (reader.hasNext()) {
            reader.next();
            count++;
        }
        outline += " elipse: " + (System.currentTimeMillis() - begin) + " for " + count;
        System.out.println(outline + " reads: " + reader.getBlockManager().getTotalRead());
        /*while (reader.hasNext()) {
            if (listRecord.size() == batchSize) {
                this.unlock();
                this.acquire();
                System.out.println("pop elements");
                this.lock();
            }
            listRecord.add(reader.next());
        }
        if (listRecord.size() > 0) {
            this.unlock();
            this.acquire();
            System.out.println("pop elements");
        }*/
    }
}

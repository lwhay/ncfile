package trev.parallel.worker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;

public class DremelsScanThread extends DremelsScanner {

    protected Schema schema;

    protected AvroColumnReader<Record> reader;

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
                Params param = new Params(new File(path));
                param.setSchema(schema);
                reader = new AvroColumnReader<>(param);
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
        try {
            reader.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        outline += " elipse: " + (System.currentTimeMillis() - begin) + " for " + count;
        System.out.println(outline);
    }
}

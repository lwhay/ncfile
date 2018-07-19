package neci.parallel;

import java.util.ArrayList;
import java.util.List;

import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

public class ScanThread implements Runnable {
    private final Schema schema;

    private final FilterBatchColumnReader<Record> reader;

    private final String path;

    //private final int batchSize;

    private List<Record> listRecord = new ArrayList<>();

    public ScanThread(FilterBatchColumnReader<Record> reader, Schema schema, String path) {
        this.reader = reader;
        this.schema = schema;
        this.path = path;
    }

    public List<Record> fetch() {
        return listRecord;
    }

    @Override
    public void run() {
        int count = 0;
        while (reader.hasNext()) {
            //listRecord.add(reader.next());
            Record record = reader.next();
            count++;
            //System.out.println(record.toString());
        }
        System.out.println(count + " at: " + path);
    }
}

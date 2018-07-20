package neci.parallel.worker;

import java.io.IOException;
import java.util.List;

import neci.ncfile.BatchAvroColumnWriter;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

public abstract class Builder implements Runnable {

    public abstract void init(BatchAvroColumnWriter<Record> writer, Schema schema, String path);

    public abstract void set(List<String> payload);

    public abstract void close() throws IOException;

    public abstract void merge();

    public abstract void build();
}

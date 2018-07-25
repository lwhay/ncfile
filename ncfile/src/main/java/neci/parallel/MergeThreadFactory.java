/**
 * 
 */
package neci.parallel;

import neci.ncfile.BatchAvroColumnWriter;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;
import neci.parallel.worker.MergeThread;

/**
 * @author Michael
 *
 */
public class MergeThreadFactory<T extends MergeThread> {
    final Class<T> builderClass;
    final BatchAvroColumnWriter<Record> writer;
    final Schema schema;
    final String path;

    public MergeThreadFactory(final Class<T> builderClass, BatchAvroColumnWriter<Record> writer, Schema schema,
            String path) {
        this.builderClass = builderClass;
        this.writer = writer;
        this.schema = schema;
        this.path = path;
    }

    public T create() throws InstantiationException, IllegalAccessException {
        T builder = builderClass.newInstance();
        System.out.println(builder.getClass().getName());
        builder.init(writer, schema, path);
        return builder;
    }

}

/**
 * 
 */
package neci.parallel.worker;

import java.io.File;
import java.io.IOException;

import neci.ncfile.BatchColumnReader;
import neci.ncfile.generic.GenericData.Record;

/**
 * @author Michael
 *
 */
public class MergeThread extends BuildThread implements Merger {
    private BatchColumnReader<Record> reader;

    @Override
    public void setInput(File datafile) {
        try {
            reader = new BatchColumnReader<>(datafile);
            reader.createSchema(schema);
            reader.create();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void build() {
        System.out.println(path + " self translation: "
                + reader.getRowCount(reader.getValidColumnNO(schema.getFields().get(0).name())));
        while (reader.hasNext()) {
            Record record = (Record) reader.next();
            //System.out.println(record.toString());
            try {
                writer.flush(record);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

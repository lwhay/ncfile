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
    public void set(File datafile) {
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
        System.out.println(path + " on: " + reader.getRowCount(0));
        while (reader.hasNext()) {
            Record record = reader.next();
            //System.out.println(record.toString());
            try {
                writer.append(record);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

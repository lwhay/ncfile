/**
 * 
 */
package neci.nest.col;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

/**
 * @author iclab
 *
 */
public class AvroToJson {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        OutputStream os = new FileOutputStream(new File(args[2]));
        boolean pretty = Boolean.parseBoolean(args[3]);
        long start = System.currentTimeMillis();
        DatumReader<Record> reader = new GenericDatumReader<Record>(readSchema);
        DataFileReader<Record> fileReader = new DataFileReader<Record>(file, reader);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(readSchema);
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(readSchema, os, pretty);
        int count = 0;
        while (fileReader.hasNext()) {
            Record record = fileReader.next();
            writer.write(record, encoder);
            if (count++ % 1000000 == 0) {
                System.out.print("*");
            }
        }
        encoder.flush();
        os.flush();
        os.close();
        fileReader.close();
        System.out.println("\n Elipsed: " + (System.currentTimeMillis() - start));
    }

}

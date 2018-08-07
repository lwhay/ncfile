package neci.single.scan;
import java.io.File;
import java.io.IOException;

import neci.ncfile.InsertAvroColumnReader;
import neci.ncfile.InsertAvroColumnReader.Params;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

public class ReadTest {
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema s = new Schema.Parser().parse(new File(args[1]));
        Params params = new Params(file);
        params.setSchema(s);
        long start = System.currentTimeMillis();
        InsertAvroColumnReader<Record> reader = new InsertAvroColumnReader<Record>(params);
        int x = 0;

        while (reader.hasNext()) {
            reader.next();
            x++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("********" + x + "\ttime: " + (end - start));
    }
}

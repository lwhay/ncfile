/**
 * 
 */
package neci.single;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;

/**
 * @author Michael
 *
 */
public class TrevScan {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Schema schema = (new Schema.Parser()).parse(new File(args[0]));

        String path = args[1];
        String outline = "";
        System.out.println(path);
        outline = "\tBegin path: " + path + " ";
        long begin = System.currentTimeMillis();
        Params param = new Params(new File(path));
        param.setSchema(schema);
        AvroColumnReader<Record> reader = new AvroColumnReader<>(param);
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

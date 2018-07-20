/**
 * 
 */
package neci.worker;

import java.io.IOException;
import java.nio.ByteBuffer;

import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Group;
import neci.ncfile.generic.GenericData.Record;
import neci.parallel.worker.BuildThread;

/**
 * @author Michael
 *
 */
public class GroupBuilder extends BuildThread {

    /**
     * @param args
     */
    public void build() {
        System.out.println(path + " on: " + payload.size());
        String gsString = "{\"name\": \"group\", \"type\": \"group\",\r\n" + "      \"fields\": [\r\n"
                + "         {\"name\": \"l_quantity\", \"type\": \"float\"},\r\n"
                + "         {\"name\": \"l_extendedprice\", \"type\": \"float\"},\r\n"
                + "         {\"name\": \"l_discount\", \"type\": \"float\"},\r\n"
                + "         {\"name\": \"l_shipdate\", \"type\": \"string\"}\r\n" + "]}";
        Schema gs = new Schema.Parser().parse(gsString);
        for (String line : payload) {
            Record record = new Record(schema);
            String[] fields = line.split("\\|");
            for (int i = 0; i < 3; i++) {
                record.put(i, Long.parseLong(fields[i]));
            }
            record.put(3, Integer.parseInt(fields[3]));
            record.put(4, Float.parseFloat(fields[7]));
            Group group = new Group(gs);
            group.put(0, Float.parseFloat(fields[4]));
            group.put(1, Float.parseFloat(fields[5]));
            group.put(2, Float.parseFloat(fields[6]));
            group.put(3, fields[10]);
            record.put(5, group);
            record.put(6, ByteBuffer.wrap(fields[8].getBytes()));
            record.put(7, ByteBuffer.wrap(fields[9].getBytes()));
            record.put(8, fields[11]);
            record.put(9, fields[12]);
            record.put(10, fields[13]);
            record.put(11, fields[14]);
            record.put(12, fields[15]);
            try {
                writer.append(record);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}

package neci.ncfile.base;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import neci.ncfile.generic.GenericData.Group;
import neci.ncfile.generic.GenericData.Record;

public class SchemaTest {
    public static void main(String[] args) throws IOException {
        File f = new File("./src/resources/group/schema.avsc");
        Schema s = new Schema.Parser().parse(f);
        System.out.println(s.toString());
        Record r = new Record(s);
        r.put(0, 1);
        r.put(1, 2);
        r.put(2, 3);
        r.put(3, 4);
        r.put(4, 1.2);
        r.put(5, 2.2);
        r.put(6, 3.2);
        r.put(7, 4.2);
        Schema gs = new Schema.Parser().parse(
                "{\"name\": \"group\", \"type\": \"group\",  \"fields\": [ {\"name\": \"l_returnflag\", \"type\": \"bytes\"}, {\"name\": \"l_linestatus\", \"type\": \"bytes\"}, {\"name\": \"l_shipdate\", \"type\": \"string\"}, {\"name\": \"l_commitdate\", \"type\": \"string\"}, {\"name\": \"l_receiptdate\", \"type\": \"string\"}]}");

        Group g = new Group(gs);
        g.put(0, ByteBuffer.wrap(new String("l_returnflag").getBytes()));
        g.put(1, ByteBuffer.wrap(new String("l_linestatus").getBytes()));
        g.put(2, "l_shipdate");
        g.put(3, "l_commitdate");
        g.put(4, "l_receiptdate");
        r.put(8, g);
        r.put(9, "l_shipinstruct");
        r.put(10, "l_shipmode");
        r.put(11, "l_comment");
        System.out.println("####\n" + r.toString());
    }
}

package neci.ncfile.base;

import java.io.File;
import java.io.IOException;

public class SchemaTest {
    public static void main(String[] args) throws IOException {
        File f = new File("/home/ly/avroppsl/lay1/schema.avsc");
        Schema s = new Schema.Parser().parse(f);
        System.out.println(s.toString());
    }
}

package columnar;

import java.io.File;
import java.io.IOException;

public class InsertColumnFileReader extends BatchColumnFileReader {
    public InsertColumnFileReader() {

    }

    public InsertColumnFileReader(File file) throws IOException {
        super(file);
    }
}

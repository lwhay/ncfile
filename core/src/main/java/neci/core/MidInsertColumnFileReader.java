package neci.core;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.trevni.InputFile;

public class MidInsertColumnFileReader extends InsertColumnFileReader {
    private int[] keyLen;

    public MidInsertColumnFileReader(File file) throws IOException {
        super();
        dataFile = new InputFile(file);
        headFile = new InputFile(
                new File(file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + ".head"));
        readHeader();
    }

    public void readHeader() throws IOException {
        InputBuffer in = new InputBuffer(headFile, 0);
        readMagic(in);
        this.rowCount = in.readFixed32();
        this.columnCount = in.readFixed32();
        int layer = in.readFixed32();
        keyLen = new int[layer];
        for (int i = 0; i < layer; i++)
            keyLen[i] = in.readFixed32();
        this.metaData = FileMetaData.read(in);
        this.columnsByName = new HashMap<String, Integer>(columnCount);

        columns = new ColumnDescriptor[columnCount];
        readFileColumnMetaData(in);
        readColumnStarts(in);
    }

}

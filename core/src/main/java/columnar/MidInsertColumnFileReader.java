package columnar;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.trevni.InputFile;

import io.InputBuffer;
import metadata.FileMetaData;

public class MidInsertColumnFileReader extends InsertColumnFileReader {
    private int[] keyLen;

    public MidInsertColumnFileReader(File file) throws IOException {
        super();
        dataFile = new InputFile(file);
        headFile = new InputFile(
                new File(file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + ".head"));
        InputBuffer headerBuffer = readHeader();
        this.bm = new BlockManager(BatchColumnFileReader.DEFAULT_BLOCK_SIZE, BlockManager.QUEUE_LENGTH_HIGH_THRESHOLD, columnCount);
        readColumns(headerBuffer);
    }

    public InputBuffer readHeader() throws IOException {
        InputBuffer in = new InputBuffer(bm, headFile, 0);
        readMagic(in);
        this.rowCount = in.readFixed32();
        this.columnCount = in.readFixed32();
        int layer = in.readFixed32();
        keyLen = new int[layer];
        for (int i = 0; i < layer; i++)
            keyLen[i] = in.readFixed32();
        this.metaData = new FileMetaData();
        this.metaData.read(in, metaData);
        this.columnsByName = new HashMap<String, Integer>(columnCount);
        return in;
    }

    public void readColumns(InputBuffer in) throws IOException {
        columns = new ColumnDescriptor[columnCount];
        readFileColumnMetaData(in);
        readColumnStarts(in);
    }

}

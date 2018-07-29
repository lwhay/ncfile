package columnar;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.trevni.Input;
import org.apache.trevni.InputFile;
import org.apache.trevni.TrevniRuntimeException;

import io.InputBuffer;
import metadata.FileColumnMetaData;
import metadata.FileMetaData;

public class BatchColumnFileReader implements Closeable {
    public final static int DEFAULT_BLOCK_SIZE = 8;
    protected Input headFile;
    protected Input dataFile;
    protected BlockManager bm;
    protected int rowCount;
    protected int columnCount;
    protected FileMetaData metaData;
    protected ColumnDescriptor[] columns;
    protected HashMap<String, Integer> columnsByName;

    public BatchColumnFileReader() {

    }

    public BatchColumnFileReader(File file) throws IOException {
        this(new InputFile(file), new InputFile(
                new File(file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + ".head")));
    }

    public BatchColumnFileReader(Input data, Input head) throws IOException {
        this.dataFile = data;
        this.headFile = head;
        InputBuffer headerBuffer = readHeader();
        this.bm = new BlockManager(DEFAULT_BLOCK_SIZE, BlockManager.DEFAULT_SCALE, columnCount);
        readColumns(headerBuffer);
    }

    public BatchColumnFileReader(File file, int bs) throws IOException {
        this(new InputFile(file), new InputFile(
                new File(file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + ".head")), bs);
    }

    public BatchColumnFileReader(Input data, Input head, int bs) throws IOException {
        this.dataFile = data;
        this.headFile = head;
        InputBuffer headerBuffer = readHeader();
        this.bm = new BlockManager(bs, BlockManager.DEFAULT_SCALE, columnCount);
        readColumns(headerBuffer);
    }

    public BlockManager getBlockManager() {
        return bm;
    }

    public int getRowCount() {
        return rowCount;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public FileMetaData getMetaData() {
        return metaData;
    }

    /**
     * Return all columns' metadata.
     */
    public FileColumnMetaData[] getFileColumnMetaData() {
        FileColumnMetaData[] result = new FileColumnMetaData[columnCount];
        for (int i = 0; i < columnCount; i++)
            result[i] = columns[i].metaData;
        return result;
    }

    public List<FileColumnMetaData> getRoots() {
        List<FileColumnMetaData> result = new ArrayList<FileColumnMetaData>();
        for (int i = 0; i < columnCount; i++)
            if (columns[i].metaData.getParent() == null)
                result.add(columns[i].metaData);
        return result;
    }

    public FileColumnMetaData getFileColumnMetaData(int number) {
        return columns[number].metaData;
    }

    /**
     * Return a column's metadata.
     */
    public FileColumnMetaData getFileColumnMetaData(String name) {
        return getColumn(name).metaData;
    }

    public int getColumnNumber(String name) {
        if ((columnsByName.get(name)) == null)
            throw new TrevniRuntimeException("No column named: " + name);
        return columnsByName.get(name);
    }

    private <T extends Comparable> ColumnDescriptor<T> getColumn(String name) {
        return (ColumnDescriptor<T>) columns[getColumnNumber(name)];
    }

    private InputBuffer readHeader() throws IOException {
        InputBuffer in = new InputBuffer(bm, headFile, 0);
        readMagic(in);
        this.rowCount = in.readFixed32();
        this.columnCount = in.readFixed32();
        this.metaData = new FileMetaData();
        this.metaData.read(in, metaData);
        this.columnsByName = new HashMap<String, Integer>(columnCount);
        return in;
    }

    private void readColumns(InputBuffer in) throws IOException {
        long begin = System.nanoTime();
        columns = new ColumnDescriptor[columnCount];
        readFileColumnMetaData(in);
        bm.colBlockTime += (System.nanoTime() - begin);
        begin = System.nanoTime();
        readColumnStarts(in);
        bm.colStartTime += (System.nanoTime() - begin);
    }

    public HashMap<String, Integer> getColumnsByName() {
        return columnsByName;
    }

    protected void readMagic(InputBuffer in) throws IOException {
        byte[] magic = new byte[InsertColumnFileWriter.MAGIC.length];
        try {
            in.readFully(magic);
        } catch (IOException e) {
            throw new IOException("Not a neci file.");
        }
        if (!(Arrays.equals(InsertColumnFileWriter.MAGIC, magic)))
            throw new IOException("Not a neci file.");
    }

    protected void readFileColumnMetaData(InputBuffer in) throws IOException {
        for (int i = 0; i < columnCount; i++) {
            FileColumnMetaData meta = new FileColumnMetaData();
            meta.read(in, this);
            meta.setDefaults(this.metaData);
            int blockCount = in.readFixed32();
            CompressedBlockDescriptor[] blocks = new CompressedBlockDescriptor[blockCount];
            for (int j = 0; j < blockCount; j++) {
                blocks[j] = new CompressedBlockDescriptor();
                blocks[j].read(in);
                //          if (meta.hasIndexValues())
                //          firstValues[i] = in.<T>readValue(meta.getType());
            }
            ColumnDescriptor column = new ColumnDescriptor(dataFile, meta, bm);
            column.setBlockDescriptor(blocks);
            columns[i] = column;
            meta.setNumber(i);
            columnsByName.put(meta.getName(), i);
        }
    }

    protected void readColumnStarts(InputBuffer in) throws IOException {
        for (int i = 0; i < columnCount; i++)
            columns[i].start = in.readFixed64();
    }

    public <T extends Comparable> BlockColumnValues<T> getValues(String columnName) throws IOException {
        return new BlockColumnValues<T>(getColumn(columnName));
    }

    public <T extends Comparable> BlockColumnValues<T> getValues(int column) throws IOException {
        return new BlockColumnValues<T>(columns[column]);
    }

    @Override
    public void close() throws IOException {
        headFile.close();
        dataFile.close();
    }

}

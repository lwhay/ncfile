package columnar;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import io.BlockOutputBuffer;
import metadata.FileColumnMetaData;
import metadata.FileMetaData;
import misc.ValueType;

public class InsertColumnFileWriter extends BatchColumnFileWriter {
    private InsertColumnFileReader[] readers;
    private String path;
    //    private int[] gap;
    private RandomAccessFile gapFile;
    //    private int[] nest;
    private RandomAccessFile nestFile;

    public static class Blocks {
        private List<BlockDescriptor> blocks;

        Blocks() {
            blocks = new ArrayList<BlockDescriptor>();
        }

        void add(BlockDescriptor b) {
            blocks.add(b);
        }

        void clear() {
            blocks.clear();
        }

        public int size() {
            return blocks.size();
        }

        BlockDescriptor get(int i) {
            return blocks.get(i);
        }
    }

    public static class ListArr {
        private List<Object> x;

        public ListArr() {
            this.x = new ArrayList<Object>();
        }

        public void add(Object o) {
            this.x.add(o);
        }

        public void clear() {
            x.clear();
        }

        public Object get(int i) {
            return x.get(i);
        }

        public Object[] toArray() {
            return x.toArray();
        }

        public int size() {
            return x.size();
        }
    }

    // public static void MemPrint(){
    //     System.out.println("$$$$$$$$$\t"+(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    // }

    //  public InsertColumnFileWriter(File fromfile, ListArr[] sort) throws IOException {
    //    this.reader = new InsertColumnFileReader(fromfile);
    //    this.insert = sort;
    //    this.filemeta = reader.getMetaData();
    //    this.meta = reader.getFileColumnMetaData();
    //    this.addRow = sort[0].size();
    //  }

    public InsertColumnFileWriter(FileMetaData filemeta, FileColumnMetaData[] meta, BlockManager bm)
            throws IOException {
        super(filemeta, meta, bm);
    }

    public void setMergeFiles(File[] files) throws IOException {
        this.files = files;
        readers = new InsertColumnFileReader[files.length];
        for (int i = 0; i < files.length; i++) {
            readers[i] = new InsertColumnFileReader(files[i]);
        }
    }

    public void setGap(String path) throws IOException {
        this.path = path;
        this.gapFile = new RandomAccessFile(path + "gap", "rw");
        this.nestFile = new RandomAccessFile(path + "nest", "rw");
    }
    //    public void setGap(int[] gap) {
    //        this.gap = gap;
    //    }

    public void mergeFiles(OutputStream head, OutputStream data) throws IOException {
        rowcount = gapFile.readInt();
        //        nest = new int[rowcount];
        //        for (int i = 0; i < rowcount; i++) {
        //            nest[i] = 1;
        //        }
        for (int i = 0; i < columncount; i++) {
            if (meta[i].isArray()) {
                mergeArrayColumn(data, i);
            } else {
                mergeColumn(data, i);
            }
        }
        writeHeader(head);
        for (int i = 0; i < readers.length; i++) {
            readers[i].close();
            readers[i] = null;
            //            files[i].delete();
            //            new File(files[i].getAbsolutePath().substring(0, files[i].getAbsolutePath().lastIndexOf(".")) + ".head")
            //                    .delete();
        }
        clearGap();
    }

    private void mergeColumn(OutputStream out, int column) throws IOException {
        BlockOutputBuffer buf = new BlockOutputBuffer(bm.getBlockSize());
        gapFile.seek(4);
        nestFile.seek(0);
        int row = 0;
        BlockColumnValues[] values = new BlockColumnValues[readers.length];
        for (int i = 0; i < readers.length; i++) {
            values[i] = readers[i].getValues(column);
        }
        ValueType type = meta[column].getType();

        for (int i = 0; i < rowcount; i++) {
            int index = gapFile.readInt();
            int nest = nestFile.readInt();
            //            int index = gap[i];
            for (int j = 0; j < nest; j++) {
                if (buf.isFull()) {
                    CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
                    blocks[column].add(b);
                    row = 0;
                    buf.writeTo(out);
                    buf.reset();
                }
                values[index].startRow();
                buf.writeValue(values[index].nextValue(), type);
                row++;
            }
        }

        if (buf.size() != 0) {
            CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
            blocks[column].add(b);
            buf.writeTo(out);
        }

        buf.close();
    }

    private void mergeArrayColumn(OutputStream out, int column) throws IOException {
        BlockOutputBuffer buf = new BlockOutputBuffer(bm.getBlockSize());
        gapFile.seek(4);
        nestFile.seek(0);
        int row = 0;
        BlockColumnValues[] values = new BlockColumnValues[readers.length];
        for (int i = 0; i < readers.length; i++) {
            values[i] = readers[i].getValues(column);
        }
        RandomAccessFile tmpNestFile = new RandomAccessFile(path + "tmpnest", "rw");

        int tmp = 0;
        for (int i = 0; i < rowcount; i++) {
            int index = gapFile.readInt();
            int nest = nestFile.readInt();
            //            int index = gap[i];
            //            nest[i] = 0;
            int tmpnest = 0;
            for (int j = 0; j < nest; j++) {
                if (buf.isFull()) {
                    CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
                    blocks[column].add(b);
                    row = 0;
                    buf.writeTo(out);
                    buf.reset();
                }
                values[index].startRow();
                int length = values[index].nextLength();
                //                nest[i] += length;
                tmpnest += length;
                tmp += length;
                buf.writeLength(tmp); //stored the array column incremently.
                row++;
            }
            tmpNestFile.writeInt(tmpnest);
        }

        nestFile.close();
        nestFile = null;
        tmpNestFile.close();
        tmpNestFile = null;
        new File(path + "nest").delete();
        new File(path + "tmpnest").renameTo(new File(path + "nest"));
        nestFile = new RandomAccessFile(path + "nest", "rw");

        if (buf.size() != 0) {
            CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
            blocks[column].add(b);
            buf.writeTo(out);
        }

        buf.close();
    }

    public void clearGap() throws IOException {
        gapFile.close();
        nestFile.close();
        gapFile = null;
        nestFile = null;
        new File(path + "gap").delete();
        new File(path + "nest").delete();
    }
}

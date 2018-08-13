package columnar;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.trevni.TrevniRuntimeException;

import codec.Codec;
import io.BlockOutputBuffer;
import io.OutputBuffer;
import io.UnionOutputBuffer;
import metadata.FileColumnMetaData;
import metadata.FileMetaData;
import misc.Blocks;
import misc.ListArr;
import misc.TranToValueType;
import misc.ValueType;

public class BatchColumnFileWriter {
    protected Codec codec;
    protected FileColumnMetaData[] meta;
    protected FileMetaData filemeta;
    protected File[] files;
    private BatchColumnFileReader[] readers;
    protected ListArr[] insert;
    protected int rowcount;
    protected int columncount;
    protected final BlockManager bm;
    //    private String path;
    //    private int[] gap;
    //    private RandomAccessFile gapFile;
    //    private int[] nest;
    //    private RandomAccessFile nestFile;
    protected long[] columnStart;
    protected Blocks[] blocks;

    protected int addRow;
    public static final byte[] MAGIC = new byte[] { 'N', 'E', 'C', 'I' };
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

    public BatchColumnFileWriter(FileMetaData filemeta, FileColumnMetaData[] meta, BlockManager bm) throws IOException {
        this.filemeta = filemeta;
        this.meta = meta;
        this.columncount = meta.length;
        this.columnStart = new long[columncount];
        this.blocks = new Blocks[columncount];
        this.bm = bm;
        for (int i = 0; i < columncount; i++) {
            blocks[i] = new Blocks();
        }
        if (filemeta.getCodec() != null) {
            this.codec = Codec.get(filemeta);
        }
    }

    protected CompressedBlockDescriptor applyCodingWithBlockDesc(int row, BlockOutputBuffer buf) throws IOException {
        int unCompressSize = buf.size();
        if (codec != null) {
            buf.compressUsing(codec);
        }
        return new CompressedBlockDescriptor(row, unCompressSize, buf.unionSize(), buf.offsetSize(), buf.payloadSize());
    }

    public void setMergeFiles(File[] files) throws IOException {
        this.files = files;
        readers = new BatchColumnFileReader[files.length];
        for (int i = 0; i < files.length; i++) {
            readers[i] = new BatchColumnFileReader(files[i]);
            // We can use Aio without skipping for better performance.
            readers[i].getBlockManager().diableAio();
        }
    }

    public void setInsert(ListArr[] sort) {
        this.insert = sort;
        this.addRow = sort[0].size();
    }

    //    public void setGap(String path) throws IOException {
    //        this.path = path;
    //        this.gapFile = new RandomAccessFile(path + "gap", "rw");
    //        this.nestFile = new RandomAccessFile(path + "nest", "rw");
    //    }
    //    public void setGap(int[] gap) {
    //        this.gap = gap;
    //    }

    public void appendTo(File file) throws IOException {
        OutputStream data = new FileOutputStream(file);
        OutputStream head = new FileOutputStream(
                new File(file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + ".head"));
        appendTo(head, data);
    }

    /*
     * write array column incremently
     */
    public void flushTo(File file) throws IOException {
        OutputStream data = new FileOutputStream(file);
        OutputStream head = new FileOutputStream(
                new File(file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + ".head"));
        flushTo(head, data);
    }

    //  public void insertTo(File file) throws IOException {
    //    OutputStream data = new FileOutputStream(file);
    //    OutputStream head = new FileOutputStream(new File(file.getPath().substring(0, file.getPath().lastIndexOf(".")) + ".head"));
    //    insertTo(head, data);
    //  }

    public void appendTo(OutputStream head, OutputStream data) throws IOException {
        rowcount = addRow;

        writeSourceColumns(data);
        writeHeader(head);
    }

    /*
     * write array column incremently
     */
    public void flushTo(OutputStream head, OutputStream data) throws IOException {
        rowcount = addRow;

        flushSourceColumns(data);
        writeHeader(head);
    }

    public void mergeFiles(File file) throws IOException {
        mergeFiles(
                new FileOutputStream(new File(
                        file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + ".head")),
                new FileOutputStream(file));
    }

    public void mergeFiles(OutputStream head, OutputStream data) throws IOException {
        //        rowcount = gapFile.readInt();
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
        }
    }

    private void mergeColumn(OutputStream out, int column) throws IOException {
        int row = 0;
        BlockColumnValues[] values = new BlockColumnValues[readers.length];
        for (int i = 0; i < readers.length; i++) {
            values[i] = readers[i].getValues(column);
        }
        if (column == 0) {
            rowcount = 0;
            for (int i = 0; i < readers.length; i++) {
                rowcount += values[i].getLastRow();
            }
        }
        ValueType type = meta[column].getType();

        if (type.equals(ValueType.UNION)) {
            UnionOutputBuffer ubuf =
                    new UnionOutputBuffer(meta[column].getUnionArray(), meta[column].getUnionBits(), bm.getBlockSize());
            for (int i = 0; i < readers.length; i++) {
                while (values[i].hasNext()) {
                    if (ubuf.isFull()) {
                        CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, ubuf);
                        blocks[column].add(b);
                        row = 0;
                        ubuf.writeTo(out);
                        ubuf.reset();
                    }
                    values[i].startRow();
                    Object x = values[i].nextValue();
                    ValueType tt = TranToValueType.tran(x);
                    Integer index = meta[column].getUnionIndex(tt);
                    if (index == null)
                        throw new TrevniRuntimeException("Illegal value type: " + tt);
                    ubuf.writeValue(x, index);
                    row++;
                }
            }
            if (ubuf.size() != 0) {
                CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, ubuf);
                blocks[column].add(b);
                ubuf.writeTo(out);
                ubuf.reset();
            }
            ubuf.close();
        } else {
            BlockOutputBuffer buf = new BlockOutputBuffer(bm.getBlockSize());
            for (int i = 0; i < readers.length; i++) {
                while (values[i].hasNext()) {
                    if (buf.isFull()) {
                        CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
                        blocks[column].add(b);
                        row = 0;
                        buf.writeTo(out);
                        buf.reset();
                    }
                    values[i].startRow();
                    buf.writeValue(values[i].nextValue(), type);
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
    }

    // Comments: there potentially exists bugs when massive compressed files are merged to a global array.
    private void mergeArrayColumn(OutputStream out, int column) throws IOException {
        //        gapFile.seek(4);
        //        nestFile.seek(0);
        int row = 0;
        BlockColumnValues[] values = new BlockColumnValues[readers.length];
        int tmp = 0;
        for (int i = 0; i < readers.length; i++) {
            values[i] = readers[i].getValues(column);
        }
        BlockOutputBuffer buf = new BlockOutputBuffer(bm.getBlockSize());
        for (int i = 0; i < readers.length; i++) {
            int length = 0;
            int localtick = 0;
            while (values[i].hasNext()) {
                if (buf.isFull()) {
                    CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
                    blocks[column].add(b);
                    row = 0;
                    buf.writeTo(out);
                    buf.reset();
                }
                values[i].startRow();
                length = values[i].nextLength();
                buf.writeLength(tmp + length); //stored the array column incrementally.
                row++;
                localtick++;
            }
            if (localtick > 0)
                System.out.println(
                        "global endOff: " + tmp + " " + localtick + "th local endOff: " + length + " row: " + row);
            else
                System.out.println(
                        "Only global endOff: " + tmp + " " + localtick + "th local endOff: " + length + " row: " + row);
            tmp += length;
        }
        //        RandomAccessFile tmpNestFile = new RandomAccessFile(path + "tmpnest", "rw");

        //        for (int i = 0; i < rowcount; i++) {
        //            int index = gapFile.readInt();
        //            int nest = nestFile.readInt();
        //            int index = gap[i];
        //            nest[i] = 0;
        //            int tmpnest = 0;
        //            for (int j = 0; j < nest; j++) {
        //                if (buf.isFull()) {
        //                    BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
        //                    blocks[column].add(b);
        //                    row = 0;
        //                    buf.writeTo(out);
        //                    buf.reset();
        //                }
        //                values[index].startRow();
        //                int length = values[index].nextLength();
        //                nest[i] += length;
        //                tmpnest += length;
        //                tmp += length;
        //                buf.writeLength(tmp); //stored the array column incremently.
        //                row++;
        //            }
        //            tmpNestFile.writeInt(tmpnest);
        //        }

        //        nestFile.close();
        //        nestFile = null;
        //        tmpNestFile.close();
        //        tmpNestFile = null;
        //        new File(path + "nest").delete();
        //        new File(path + "tmpnest").renameTo(new File(path + "nest"));
        //        nestFile = new RandomAccessFile(path + "nest", "rw");

        if (buf.size() != 0) {
            CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
            blocks[column].add(b);
            buf.writeTo(out);
        }

        buf.close();
    }

    //  public void insertTo(OutputStream head, OutputStream data) throws IOException {
    //    rowcount = addRow + reader.getRowCount();
    //    values = new ColumnValues[meta.length];
    //    for(int i = 0; i < meta.length; i++){
    //      values[i] = reader.getValues(i);
    //    }
    //
    //    writeColumns(data);
    //    writeHeader(head);
    //  }

    private void writeSourceColumns(OutputStream out) throws IOException {
        BlockOutputBuffer buf = new BlockOutputBuffer(bm.getBlockSize());
        for (int i = 0; i < columncount; i++) {
            ValueType type = meta[i].getType();
            int row = 0;
            if (meta[i].isArray()) {
                for (Object x : insert[i].toArray()) {
                    if (buf.isFull()) {
                        CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
                        blocks[i].add(b);
                        row = 0;
                        buf.writeTo(out);
                        buf.reset();
                    }
                    buf.writeLength((Integer) x);
                    row++;
                }
            } else if (type.equals(ValueType.UNION)) {
                UnionOutputBuffer ubuf =
                        new UnionOutputBuffer(meta[i].getUnionArray(), meta[i].getUnionBits(), bm.getBlockSize());
                for (Object x : insert[i].toArray()) {
                    if (ubuf.isFull()) {
                        CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, ubuf);
                        blocks[i].add(b);
                        row = 0;
                        ubuf.writeTo(out);
                        ubuf.reset();
                    }
                    ValueType tt = TranToValueType.tran(x);
                    Integer index = meta[i].getUnionIndex(tt);
                    if (index == null)
                        throw new TrevniRuntimeException("Illegal value type: " + tt);
                    ubuf.writeValue(x, index);
                    row++;
                }
                if (ubuf.size() != 0) {
                    CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, ubuf);
                    blocks[i].add(b);
                    ubuf.writeTo(out);
                    ubuf.reset();
                }
                ubuf.close();
            } else {
                for (Object x : insert[i].toArray()) {
                    if (buf.isFull()) {
                        CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
                        blocks[i].add(b);
                        row = 0;
                        buf.writeTo(out);
                        buf.reset();
                    }
                    buf.writeValue(x, type);
                    row++;
                }
            }

            insert[i].clear();

            if (buf.size() != 0) {
                CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
                blocks[i].add(b);
                buf.writeTo(out);
                buf.reset();
            }
        }
        insert = null;
        buf.close();
    }

    /*
     * write array column incremently
     */
    private void flushSourceColumns(OutputStream out) throws IOException {
        BlockOutputBuffer buf = new BlockOutputBuffer(bm.getBlockSize());
        for (int i = 0; i < columncount; i++) {
            ValueType type = meta[i].getType();
            int row = 0;
            if (meta[i].isArray()) {
                int tmp = 0;
                for (Object x : insert[i].toArray()) {
                    if (buf.isFull()) {
                        CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
                        blocks[i].add(b);
                        row = 0;
                        buf.writeTo(out);
                        buf.reset();
                    }
                    tmp += (int) x;
                    buf.writeLength((Integer) tmp);
                    row++;
                }
            } else if (type.equals(ValueType.UNION)) {
                UnionOutputBuffer ubuf =
                        new UnionOutputBuffer(meta[i].getUnionArray(), meta[i].getUnionBits(), bm.getBlockSize());
                for (Object x : insert[i].toArray()) {
                    if (ubuf.isFull()) {
                        CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, ubuf);
                        blocks[i].add(b);
                        row = 0;
                        ubuf.writeTo(out);
                        ubuf.reset();
                    }
                    ValueType tt = TranToValueType.tran(x);
                    Integer index = meta[i].getUnionIndex(tt);
                    if (index == null)
                        throw new TrevniRuntimeException("Illegal value type: " + tt);
                    ubuf.writeValue(x, index);
                    row++;
                }
                if (ubuf.size() != 0) {
                    CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, ubuf);
                    blocks[i].add(b);
                    ubuf.writeTo(out);
                    ubuf.reset();
                }
                ubuf.close();
            } else {
                for (Object x : insert[i].toArray()) {
                    if (buf.isFull()) {
                        CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
                        blocks[i].add(b);
                        row = 0;
                        buf.writeTo(out);
                        buf.reset();
                    }
                    buf.writeValue(x, type);
                    row++;
                }
            }

            insert[i].clear();

            if (buf.size() != 0) {
                CompressedBlockDescriptor b = applyCodingWithBlockDesc(row, buf);
                blocks[i].add(b);
                buf.writeTo(out);
                buf.reset();
            }
        }
        insert = null;
        buf.close();
    }

    public void writeHeader(OutputStream out) throws IOException {
        OutputBuffer header = new OutputBuffer();
        header.write(MAGIC);
        header.writeFixed32(rowcount);
        header.writeFixed32(columncount);
        filemeta.write(header);
        int i = 0;
        long delay = 0;
        for (FileColumnMetaData c : meta) {
            columnStart[i] = delay;
            c.write(header);
            int size = blocks[i].size();
            header.writeFixed32(size);
            for (int k = 0; k < size; k++) {
                blocks[i].get(k).writeTo(header);
                delay += ((CompressedBlockDescriptor) (blocks[i].get(k))).getCompressedSize();
            }
            blocks[i].clear();
            i++;
        }

        for (i = 0; i < columncount; i++) {
            header.writeFixed64(columnStart[i]);
        }
        header.writeTo(out);
        header.close();
    }

    //    public void clearGap() throws IOException {
    //        gapFile.close();
    //        nestFile.close();
    //        gapFile = null;
    //        nestFile = null;
    //        new File(path + "gap").delete();
    //        new File(path + "nest").delete();
    //    }
}

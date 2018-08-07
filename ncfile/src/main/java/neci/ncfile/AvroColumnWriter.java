package neci.ncfile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import columnar.BlockDescriptor;
import columnar.BlockManager;
import columnar.InsertColumnFileWriter;
import io.BlockOutputBuffer;
import io.OutputBuffer;
import metadata.FileColumnMetaData;
import metadata.FileMetaData;
import neci.ncfile.base.Schema;

public class AvroColumnWriter {
    protected FileColumnMetaData[] meta;
    protected FileMetaData filemeta;
    protected OutputStream data;
    protected OutputStream head;
    protected int rowcount;
    protected int columncount;
    protected long[] columnStart;
    protected Blocks[] blocks;
    protected final BlockManager bm;
    BlockOutputBuffer buf;
    int index;

    public static final String SCHEMA_KEY = "avro.schema";

    class Blocks {
        private List<BlockDescriptor> blocks;

        Blocks() {
            blocks = new ArrayList<BlockDescriptor>();
        }

        List<BlockDescriptor> get() {
            return blocks;
        }

        void add(BlockDescriptor b) {
            blocks.add(b);
        }

        void clear() {
            blocks.clear();
        }

        BlockDescriptor get(int i) {
            return blocks.get(i);
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

    public AvroColumnWriter(Schema schema, String path, int blockSize) throws IOException {
        AvroColumnator columnator = new AvroColumnator(schema);
        filemeta = new FileMetaData();
        filemeta.set(SCHEMA_KEY, schema.toString());
        meta = columnator.getColumns();
        this.bm = new BlockManager(blockSize, BlockManager.QUEUE_LENGTH_HIGH_THRESHOLD, meta.length);
        this.columncount = meta.length;
        this.columnStart = new long[columncount];
        this.blocks = new Blocks[columncount];
        for (int i = 0; i < columncount; i++) {
            blocks[i] = new Blocks();
        }
        data = new FileOutputStream(new File(path));
        head = new FileOutputStream(new File(path.substring(0, path.lastIndexOf(".")) + ".head"));
        buf = new BlockOutputBuffer(bm.getBlockSize());
        index = 0;
        rowcount = 0;
    }

    public BlockManager getBlockManager() {
        return bm;
    }

    public void writeColumn(int columnNo, Object value) throws IOException {
        if (buf.isFull()) {
            BlockDescriptor b = new BlockDescriptor(index, buf.size(), buf.size());
            blocks[columnNo].add(b);
            index = 0;
            buf.writeTo(data);
            buf.reset();
        }
        //        ValueType tp = meta[columnNo].getType();
        //        switch (tp) {
        //            case FIXED32:
        //            case INT:
        //                buf.writeValue(Integer.parseInt(value.toString()), tp);
        //                break;
        //            case FIXED64:
        //            case LONG:
        //                buf.writeValue(Long.parseLong(value.toString()), tp);
        //                break;
        //            case FLOAT:
        //                buf.writeValue(Float.parseFloat(value.toString()), tp);
        //                break;
        //            case DOUBLE:
        //                buf.writeValue(Double.parseDouble(value.toString()), tp);
        //                break;
        //            default:
        //                buf.writeValue(value, tp);
        //                break;
        //        }
        buf.writeValue(value, meta[columnNo].getType());
        index++;
        if (columnNo == 0)
            rowcount++;
    }

    public void writeArrayColumn(int columnNo, int value) throws IOException {
        if (buf.isFull()) {
            BlockDescriptor b = new BlockDescriptor(index, buf.size(), buf.size());
            blocks[columnNo].add(b);
            index = 0;
            buf.writeTo(data);
            buf.reset();
        }
        buf.writeLength(value);
        index++;
    }

    public void flush(int columnNo) throws IOException {
        if (index > 0) {
            BlockDescriptor b = new BlockDescriptor(index, buf.size(), buf.size());
            blocks[columnNo].add(b);
            index = 0;
            buf.writeTo(data);
            buf.reset();
        }
    }

    public void close() throws IOException {
        writeHeader();
    }

    public void writeHeader() throws IOException {
        OutputBuffer header = new OutputBuffer();
        header.write(InsertColumnFileWriter.MAGIC);
        header.writeFixed32(rowcount);
        header.writeFixed32(columncount);
        filemeta.write(header);
        int i = 0;
        long delay = 0;
        for (FileColumnMetaData c : meta) {
            columnStart[i] = delay;
            c.write(header);
            int size = blocks[i].blocks.size();
            buf.writeFixed32(size);
            for (int k = 0; k < size; k++) {
                blocks[i].get(k).writeTo(header);
                delay += blocks[i].get(k).getSize();
            }
            blocks[i].clear();
            i++;
        }

        for (i = 0; i < columncount; i++) {
            header.writeFixed64(columnStart[i]);
        }
        header.writeTo(head);
        header.close();
    }

    //  public void insertTo(File file) throws IOException {
    //    OutputStream data = new FileOutputStream(file);
    //    OutputStream head = new FileOutputStream(new File(file.getPath().substring(0, file.getPath().lastIndexOf(".")) + ".head"));
    //    insertTo(head, data);
    //  }

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

    //  private void writeColumns(OutputStream out) throws IOException {
    //    assert(gap.length == (addRow + 1));
    //    nest = new int[addRow];
    //    for (int k = 0; k < addRow; k++) {
    //      nest[k] = 1;
    //    }
    //
    //    for (int j = 0; j < columncount; j++) {
    //      if (meta[j].getType() == ValueType.NULL) {
    //        writeArrayColumn(out, j);
    //      } else {
    //        writeColumn(out, j);
    //      }
    //    }
    ////        MemPrint();
    ////    reader.close();
    ////    reader = null;
    ////    values = null;
    //    insert = null;
    //    gap = null;
    //    nest = null;
    //  }

    //  private void writeColumn(OutputStream out, int column) throws IOException {
    //    OutputBuffer buf = new OutputBuffer();
    //    int row = 0;
    //    int realrow = 0;
    //    ValueType type = meta[column].getType();
    //
    //    for (int i = 0; i < addRow; i++) {
    //      for (int k = 0; k < gap[i]; k++) {
    //        if(buf.isFull()){
    //          BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
    //          blocks[column].add(b);
    //          row = 0;
    //          buf.writeTo(out);
    //          buf.reset();
    //        }
    //        values[column].startRow();
    //        buf.writeValue(values[column].nextValue(), type);
    //        row++;
    //      }
    //      for (int r = 0; r < nest[i]; r++) {
    //        if(buf.isFull()){
    //          BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
    //          blocks[column].add(b);
    //          row = 0;
    //          buf.writeTo(out);
    //          buf.reset();
    //        }
    //        buf.writeValue(insert[column].get(realrow), type);
    //        row++;
    //        realrow++;
    //      }
    //    }
    //    assert (realrow == insert[column].size());
    //    insert[column].clear();
    //
    //    for (int k = 0; k < gap[addRow]; k++) {
    //      if(buf.isFull()){
    //        BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
    //        blocks[column].add(b);
    //        row = 0;
    //        buf.writeTo(out);
    //        buf.reset();
    //      }
    //      values[column].startRow();
    //      buf.writeValue(values[column].nextValue(), type);
    //      row++;
    //    }
    //
    //    if(buf.size() != 0){
    //      BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
    //      blocks[column].add(b);
    //      buf.writeTo(out);
    //    }
    //
    //    buf.close();
    //  }

    //  private void writeArrayColumn(OutputStream out, int column) throws IOException {
    //    OutputBuffer buf = new OutputBuffer();
    //    int row = 0;
    //    int realrow = 0;
    //    long[] tmgap = new long[addRow + 1];
    //    int[] tmnest = new int[addRow];
    //    ValueType type = meta[column].getType();
    //    if(type == ValueType.NULL){
    //      int y = 0;
    //      for(int x = 0; x <addRow; x++){
    //        for(int no = 0; no < nest[x]; no++){
    //          tmnest[x] += (Integer) insert[column].get(y + no);
    //        }
    //        y += nest[x];
    //      }
    //    }
    //
    //    for (int i = 0; i < addRow; i++) {
    //      for (long k = 0; k < gap[i]; k++) {
    //        if(buf.isFull()){
    //          BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
    //          blocks[column].add(b);
    //          row = 0;
    //          buf.writeTo(out);
    //          buf.reset();
    //        }
    //        values[column].startRow();
    //        int length = values[column].nextLength();
    //        buf.writeLength(length);
    //        tmgap[i] += length;
    //        row++;
    //      }
    //      for (int r = 0; r < nest[i]; r++) {
    //        if(buf.isFull()){
    //          BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
    //          blocks[column].add(b);
    //          row = 0;
    //          buf.writeTo(out);
    //          buf.reset();
    //        }
    //        buf.writeLength((Integer) insert[column].get(realrow));
    //        realrow++;
    //        row++;
    //      }
    //    }
    //    assert (realrow == insert[column].size());
    //    insert[column].clear();
    //
    //    for (long k = 0; k < gap[addRow]; k++) {
    //      if(buf.isFull()){
    //        BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
    //        blocks[column].add(b);
    //        row = 0;
    //        buf.writeTo(out);
    //        buf.reset();
    //      }
    //      values[column].startRow();
    //      int len = values[column].nextLength();
    //      buf.writeLength(len);
    //      tmgap[addRow] += len;
    //      row++;
    //    }
    //    nest = tmnest;
    //    gap = tmgap;
    //
    //    if(buf.size() != 0){
    //      BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
    //      blocks[column].add(b);
    //      buf.writeTo(out);
    //    }
    //
    //    buf.close();
    //  }
}

//package neci.ncfile;
//
//import java.io.IOException;
//import java.util.HashMap;
//
//import neci.core.BlockDescriptor;
//import neci.core.FileColumnMetaData;
//import neci.core.InsertColumnFileWriter;
//import neci.core.MidOutputBuffer;
//import neci.core.ValueType;
//import neci.ncfile.base.Schema;
//
//public class MidAvroColumnWriter extends AvroColumnWriter {
//    private HashMap<Integer, Integer> columnNoMatch;
//    private HashMap<Integer, Integer> levelNoMatch;
//    private MidOutputBuffer keyBuf;
//    private int layer;
//    private int[] keyLen;
//
//    public MidAvroColumnWriter(Schema schema, String path, int[] keyLen) throws IOException {
//        super(schema, path);
//        this.layer = keyLen.length;
//        this.keyLen = keyLen;
//        columnStart = new long[columncount + layer];
//        blocks = new Blocks[columncount + layer];
//        for (int i = 0; i < columncount + layer; i++) {
//            blocks[i] = new Blocks();
//        }
//
//        buf = new MidOutputBuffer();
//        keyBuf = new MidOutputBuffer();
//
//        FileColumnMetaData[] tmp = new FileColumnMetaData[meta.length + layer];
//        columnNoMatch = new HashMap<Integer, Integer>();
//        levelNoMatch = new HashMap<Integer, Integer>();
//        int m = 0;
//        tmp[0] = new FileColumnMetaData("keyGroup" + m, ValueType.KEYGROUP);
//        levelNoMatch.put(0, 0);
//        m++;
//        int in = 0;
//        while (m < layer) {
//            while (!meta[in].isArray()) {
//                tmp[in + m] = meta[in];
//                columnNoMatch.put(in, in + m);
//                in++;
//            }
//            tmp[in + m] = meta[in];
//            columnNoMatch.put(in, in + m);
//            levelNoMatch.put(m, in + m + 1);
//            m++;
//            tmp[in + m] = new FileColumnMetaData("keyGroup" + m, ValueType.KEYGROUP);
//
//        }
//        in++;
//        while (in < meta.length) {
//            tmp[in + m] = meta[in];
//            columnNoMatch.put(in, in + m);
//            in++;
//        }
//        meta = tmp;
//    }
//
//    @Override
//    public void writeColumn(int columnNo, Object value) throws IOException {
//        super.writeColumn(columnNoMatch.get(columnNo), value);
//    }
//
//    @Override
//    public void writeArrayColumn(int columnNo, int value) throws IOException {
//        super.writeArrayColumn(columnNoMatch.get(columnNo), value);
//    }
//
//    @Override
//    public void flush(int columnNo) throws IOException {
//        super.flush(columnNoMatch.get(columnNo));
//    }
//
//    public void flushFlag(int level) throws IOException {
//        int columnNo = levelNoMatch.get(level);
//        if (index > 0) {
//            BlockDescriptor b = new BlockDescriptor(index, keyBuf.size(), keyBuf.size());
//            blocks[columnNo].add(b);
//            index = 0;
//            keyBuf.writeTo(data);
//            keyBuf.reset();
//        }
//    }
//
//    public void writeFlagColumn(int level, byte flag, KeyofBTree value) throws IOException {
//        int columnNo = levelNoMatch.get(level);
//        if (buf.isFull()) {
//            BlockDescriptor b = new BlockDescriptor(index, buf.size(), buf.size());
//            blocks[columnNo].add(b);
//            index = 0;
//            buf.writeTo(data);
//            buf.reset();
//        }
//
//        keyBuf.writeKeyGroup(flag, value.getKey());
//        index++;
//        if (columnNo == 0)
//            rowcount++;
//    }
//
//    @Override
//    public void writeHeader() throws IOException {
//        buf.write(InsertColumnFileWriter.MAGIC);
//        buf.writeFixed32(rowcount);
//        buf.writeFixed32(columncount);
//        buf.writeFixed32(layer);
//        for (int i = 0; i < layer; i++)
//            buf.writeFixed32(keyLen[i]);
//        filemeta.write(buf);
//        int i = 0;
//        long delay = 0;
//        for (FileColumnMetaData c : meta) {
//            columnStart[i] = delay;
//            c.write(buf);
//            int size = blocks[i].get().size();
//            buf.writeFixed32(size);
//            for (int k = 0; k < size; k++) {
//                blocks[i].get(k).writeTo(buf);
//                delay += blocks[i].get(k).getSize();
//            }
//            blocks[i].clear();
//            i++;
//        }
//
//        for (i = 0; i < columncount; i++) {
//            buf.writeFixed64(columnStart[i]);
//        }
//        buf.writeTo(head);
//        buf.close();
//    }
//}

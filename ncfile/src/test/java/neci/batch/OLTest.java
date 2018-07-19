package neci.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import neci.ncfile.CombKey;
import neci.ncfile.ComparableKey;
import neci.ncfile.EncodeSchema;
import neci.ncfile.InsertAvroColumnWriter;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

public class OLTest {
    private static final int DEFAULT_BLOCK_KBYTES = 32;

    //public static void MemPrint(){
    //System.out.println("########\t"+(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    //}
    public static class Okey implements Comparable<Okey> {
        private long ck;
        private long ok;

        public Okey() {
            ck = ok = 0;
        }

        public Okey(long ck, long ok) {
            this.ck = ck;
            this.ok = ok;
        }

        public long getCk() {
            return this.ck;
        }

        public long getOk() {
            return this.ok;
        }

        public void setCk(long ck) {
            this.ck = ck;
        }

        public void setOk(long ok) {
            this.ok = ok;
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof Okey)) {
                throw new ClassCastException("Cannot cast to Okey!");
            }

            Okey o = (Okey) obj;
            return (compareTo(o) == 0) ? true : false;
        }

        @Override
        public int compareTo(Okey o) {
            if (this.ck > o.getCk())
                return 1;
            else if (this.ck < o.getCk())
                return -1;
            else {
                if (this.ok > o.getOk())
                    return 1;
                else if (this.ok < o.getOk())
                    return -1;
            }
            return 0;
        }
    }

    public static void olTrev(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        File oFile = new File(args[0]);
        File lFile = new File(args[1]);
        String olPath = args[2];
        String schemaPath = args[3];
        BufferedReader oReader = new BufferedReader(new FileReader(oFile));
        BufferedReader lReader = new BufferedReader(new FileReader(lFile));
        Schema olS = new Schema.Parser().parse(new File(schemaPath + "o_l.avsc"));
        Schema lS = new Schema.Parser().parse(new File(schemaPath + "lineitem.avsc"));
        int[] fs = new int[] { 1, 0 };
        if (args[4].equals("true")) {
            EncodeSchema es = new EncodeSchema(olS);
            olS = es.getEncode();
            lS = es.encode('b', lS);
        }

        if (args[5].equals("true")) {
            InsertAvroColumnWriter<CombKey, Record> writer = new InsertAvroColumnWriter<CombKey, Record>(olS, olPath,
                    fs, Integer.parseInt(args[6]), 40, DEFAULT_BLOCK_KBYTES);

            String otemp = "";
            String ltemp = "";
            int x = 0;
            while ((otemp = oReader.readLine()) != null) {
                String[] o = otemp.split("\\|");
                long ok = Long.parseLong(o[0]);
                Record orders = new Record(olS);
                orders.put(0, Long.parseLong(o[0]));
                orders.put(1, Long.parseLong(o[1]));
                orders.put(2, ByteBuffer.wrap(o[2].getBytes()));
                orders.put(3, Float.parseFloat(o[3]));
                orders.put(4, o[4]);
                orders.put(5, o[5]);
                orders.put(6, o[6]);
                orders.put(7, Integer.parseInt(o[7]));
                orders.put(8, o[8]);

                List<Record> lList = new ArrayList<Record>();
                while (true) {
                    if (ltemp == "") {
                        ltemp = lReader.readLine();
                    }
                    String[] l = ltemp.split("\\|");
                    if (Long.parseLong(l[0]) == ok) {
                        Record lineitem = new Record(lS);
                        lineitem.put(0, Long.parseLong(l[0]));
                        lineitem.put(1, Long.parseLong(l[1]));
                        lineitem.put(2, Long.parseLong(l[2]));
                        lineitem.put(3, Integer.parseInt(l[3]));
                        lineitem.put(4, Float.parseFloat(l[4]));
                        lineitem.put(5, Float.parseFloat(l[5]));
                        lineitem.put(6, Float.parseFloat(l[6]));
                        lineitem.put(7, Float.parseFloat(l[7]));
                        lineitem.put(8, ByteBuffer.wrap(l[8].getBytes()));
                        lineitem.put(9, ByteBuffer.wrap(l[9].getBytes()));
                        lineitem.put(10, l[10]);
                        lineitem.put(11, l[11]);
                        lineitem.put(12, l[12]);
                        lineitem.put(13, l[13]);
                        lineitem.put(14, l[14]);
                        lineitem.put(15, l[15]);
                        lList.add(lineitem);
                        if ((ltemp = lReader.readLine()) != null) {
                            continue;
                        }
                    }
                    break;
                }
                orders.put(9, lList);
                writer.append(new CombKey(orders, fs), orders);
                //        System.out.println(++x);
                //MemPrint();
            }
            writer.flush();
        } else {
            InsertAvroColumnWriter<ComparableKey, Record> writer = new InsertAvroColumnWriter<ComparableKey, Record>(
                    olS, olPath, fs, Integer.parseInt(args[6]), 40, DEFAULT_BLOCK_KBYTES);

            String otemp = "";
            String ltemp = "";
            int x = 0;
            while ((otemp = oReader.readLine()) != null) {
                String[] o = otemp.split("\\|");
                long ok = Long.parseLong(o[0]);
                Record orders = new Record(olS);
                orders.put(0, Long.parseLong(o[0]));
                orders.put(1, Long.parseLong(o[1]));
                orders.put(2, ByteBuffer.wrap(o[2].getBytes()));
                orders.put(3, Float.parseFloat(o[3]));
                orders.put(4, o[4]);
                orders.put(5, o[5]);
                orders.put(6, o[6]);
                orders.put(7, Integer.parseInt(o[7]));
                orders.put(8, o[8]);

                List<Record> lList = new ArrayList<Record>();
                while (true) {
                    if (ltemp == "") {
                        ltemp = lReader.readLine();
                    }
                    String[] l = ltemp.split("\\|");
                    if (Long.parseLong(l[0]) == ok) {
                        Record lineitem = new Record(lS);
                        lineitem.put(0, Long.parseLong(l[0]));
                        lineitem.put(1, Long.parseLong(l[1]));
                        lineitem.put(2, Long.parseLong(l[2]));
                        lineitem.put(3, Integer.parseInt(l[3]));
                        lineitem.put(4, Float.parseFloat(l[4]));
                        lineitem.put(5, Float.parseFloat(l[5]));
                        lineitem.put(6, Float.parseFloat(l[6]));
                        lineitem.put(7, Float.parseFloat(l[7]));
                        lineitem.put(8, ByteBuffer.wrap(l[8].getBytes()));
                        lineitem.put(9, ByteBuffer.wrap(l[9].getBytes()));
                        lineitem.put(10, l[10]);
                        lineitem.put(11, l[11]);
                        lineitem.put(12, l[12]);
                        lineitem.put(13, l[13]);
                        lineitem.put(14, l[14]);
                        lineitem.put(15, l[15]);
                        lList.add(lineitem);
                        if ((ltemp = lReader.readLine()) != null) {
                            continue;
                        }
                    }
                    break;
                }
                orders.put(9, lList);
                writer.append(new ComparableKey(orders, fs), orders);
                //        System.out.println(++x);
                //MemPrint();
            }
            writer.flush();
        }
        lReader.close();
        oReader.close();
        long end = System.currentTimeMillis();
        System.out.println("yuan:" + (end - start));
    }

    public static void main(String[] args) throws IOException {
        olTrev(args);
    }
}

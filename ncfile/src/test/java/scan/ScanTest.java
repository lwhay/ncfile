package scan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import neci.ncfile.BatchAvroColumnWriter;
import neci.ncfile.BatchColumnReader;
import neci.ncfile.base.Schema;
import neci.ncfile.base.Schema.Field;
import neci.ncfile.generic.GenericData.Record;

public class ScanTest {
    private static final int DEFAULT_BLOCK_KBYTES = 32;

    public static boolean match(String s, String left, String right) {
        if (left == null)
            return s.compareTo(right) < 0;
        else {
            if (right == null)
                return s.compareTo(left) > 0;
            else
                return s.compareTo(left) > 0 && s.compareTo(right) < 0;
        }
    }

    public static boolean leftMatch(String s, String left) {
        return s.compareTo(left) > 0;
    }

    public static boolean rightMatch(String s, String right) {
        return s.compareTo(right) < 0;
    }

    public static void loadLineitem(int bs, int free, int mul, String lPath, String resultPath, String sPath,
            List<Field> fs, Schema s) throws IOException {
        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, resultPath, free, mul, bs);
        BufferedReader reader = new BufferedReader(new FileReader(lPath));
        String line = "";
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record r = new Record(s);
            for (int i = 0; i < fs.size(); i++) {
                switch (fs.get(i).schema().getType()) {
                    case BOOLEAN:
                        r.put(i, Boolean.parseBoolean(tmp[i]));
                        break;
                    case INT:
                        r.put(i, Integer.parseInt(tmp[i]));
                        break;
                    case LONG:
                        r.put(i, Long.parseLong(tmp[i]));
                        break;
                    case STRING:
                        r.put(i, tmp[i]);
                        break;
                    case FLOAT:
                        r.put(i, Float.parseFloat(tmp[i]));
                        break;
                    case DOUBLE:
                        r.put(i, Double.parseDouble(tmp[i]));
                        break;
                    case BYTES:
                        r.put(i, ByteBuffer.wrap(tmp[i].getBytes()));
                        break;
                }
            }
            writer.append(r);
        }
        reader.close();
        int index = writer.flush();
        File[] files = new File[index];
        for (int i = 0; i < index; i++)
            files[i] = new File(resultPath + "file" + String.valueOf(i) + ".trv");
        if (index == 1) {
            new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
            new File(resultPath + "file0.trv").renameTo(new File(resultPath + "result.trv"));
            //            ColumnReader rr = new ColumnReader<Record>(new File(resultPath + "result.trv"));
            //            rr.createSchema(s);
            //            Integer[] in = new Integer[level];
            //            for (int m = 0; m < level; m++)
            //                in[m] = 0;
            //            while (reader.hasNext()) {
            //                Record record = reader.next();
            //                updateTree(0, null, in, record);
            //            }
            //            for (int i = 0; i < level; i++)
            //                tree[i].write();
        } else {
            //            merge(files);
            //            BatchSortTrevniReader re = new BatchSortTrevniReader(files, keySchema, tmpPath);
            //            while (re.hasNext()) {
            //                Record record = re.next().getRecord();
            //            }
            //            re.close();
            //            re = null;
            writer.mergeFiles(files);
        }
    }

    public static void neciRecordA1(String resultPath, String sPath, String args, int max) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan1.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int no1 = reader.getColumnNO("l_shipdate");
        BitSet set = new BitSet(reader.getRowCount(no1));
        int i = 0;
        while (reader.hasNext(no1)) {
            String tm = reader.nextValue(no1).toString();
            if (tm.compareTo(args) < 0) {
                set.set(i);
            }
            i++;
        }

        int all = set.cardinality();
        int res = all;

        int sta = 0;
        while (max < all) {
            Record[] xxx = new Record[max];
            for (int x = 0; x < max; x++) {
                xxx[x] = new Record(s);
            }

            int[] nn = new int[fs.size()];
            for (int j = 0; j < fs.size(); j++) {
                Field f = fs.get(j);
                int no = reader.getColumnNO(f.name());
                int st = sta;
                int mm = 0;
                while (mm < max) {
                    i = set.nextSetBit(st);
                    xxx[nn[j]].put(j, reader.readValue(no, i));
                    nn[j]++;
                    mm++;
                    st = i + 1;
                }
            }
            all -= max;
            sta = i + 1;
        }

        if (all > 0) {
            Record[] xxx = new Record[all];
            for (int x = 0; x < all; x++) {
                xxx[x] = new Record(s);
            }

            int[] nn = new int[fs.size()];
            for (int j = 0; j < fs.size(); j++) {
                Field f = fs.get(j);
                int no = reader.getColumnNO(f.name());
                i = set.nextSetBit(sta);
                while (i != -1) {
                    xxx[nn[j]].put(j, reader.readValue(no, i));
                    nn[j]++;
                    if (++i >= set.length())
                        break;
                    i = set.nextSetBit(i);
                }
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("A1 neciRecord scan time: " + (end - start));
        System.out.println("res: " + res);
    }

    public static void neciScanA1(String resultPath, String sPath, String args) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan1.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int no1 = reader.getColumnNO("l_shipdate");
        BitSet set = new BitSet(reader.getRowCount(no1));
        int i = 0;
        int mm = 0;
        while (reader.hasNext(no1)) {
            String tm = reader.nextValue(no1).toString();
            if (tm.compareTo(args) < 0) {
                set.set(i);
                mm++;
            }
            i++;
        }

        int[] nn = new int[fs.size()];
        for (int j = 0; j < fs.size(); j++) {
            Field f = fs.get(j);
            int no = reader.getColumnNO(f.name());

            i = set.nextSetBit(0);
            while (i != -1) {
                reader.readValue(no, i);
                nn[j]++;
                if (++i >= set.length())
                    break;
                i = set.nextSetBit(i);
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("neci A1 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void neciNoA1(String resultPath, String sPath, String args) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan1.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int no1 = reader.getColumnNO("l_shipdate");
        BitSet set = new BitSet(reader.getRowCount(no1));
        int i = 0;
        int mm = 0;
        while (reader.hasNext(no1)) {
            String tm = reader.nextValue(no1).toString();
            if (tm.compareTo(args) < 0) {
                set.set(i);
                mm++;
            }
            i++;
        }

        int[] nn = new int[fs.size()];
        for (int j = 0; j < fs.size(); j++) {
            Field f = fs.get(j);
            int no = reader.getColumnNO(f.name());

            i = set.nextSetBit(0);
            while (i != -1) {
                reader.skipValue(no, i);
                nn[j]++;
                if (++i >= set.length())
                    break;
                i = set.nextSetBit(i);
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("neci A1 no time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void neciRecordA2(String resultPath, String sPath, String args1, String args2, int max)
            throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan2.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);

        int no1 = reader.getColumnNO("l_shipdate");
        BitSet set = new BitSet(reader.getRowCount(no1));
        int i = 0;
        while (reader.hasNext(no1)) {
            if (reader.nextValue(no1).toString().compareTo(args1) < 0) {
                set.set(i);
            }
            i++;
        }

        int no2 = reader.getColumnNO("l_commitdate");
        BitSet set2 = new BitSet(reader.getRowCount(no2));
        i = 0;
        while (reader.hasNext(no2)) {
            if (reader.nextValue(no2).toString().compareTo(args2) < 0) {
                set2.set(i);
            }
            i++;
        }
        set.and(set2);
        int all = set.cardinality();
        int res = all;

        int sta = 0;
        while (max < all) {
            Record[] xxx = new Record[max];
            for (int x = 0; x < max; x++) {
                xxx[x] = new Record(s);
            }

            int[] nn = new int[fs.size()];
            for (int j = 0; j < fs.size(); j++) {
                Field f = fs.get(j);
                int no = reader.getColumnNO(f.name());
                int st = sta;
                int mm = 0;
                while (mm < max) {
                    i = set.nextSetBit(st);
                    xxx[nn[j]].put(j, reader.readValue(no, i));
                    nn[j]++;
                    mm++;
                    st = i + 1;
                }
            }
            all -= max;
            sta = i + 1;
        }

        if (all > 0) {
            Record[] xxx = new Record[all];
            for (int x = 0; x < all; x++) {
                xxx[x] = new Record(s);
            }

            int[] nn = new int[fs.size()];
            for (int j = 0; j < fs.size(); j++) {
                Field f = fs.get(j);
                int no = reader.getColumnNO(f.name());
                i = set.nextSetBit(sta);
                while (i != -1) {
                    xxx[nn[j]].put(j, reader.readValue(no, i));
                    nn[j]++;
                    if (++i >= set.length())
                        break;
                    i = set.nextSetBit(i);
                }
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("A2 neciRecord scan time: " + (end - start));
        System.out.println("res: " + res);
    }

    public static void neciScanA2(String resultPath, String sPath, String args1, String args2) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan2.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);

        int no1 = reader.getColumnNO("l_shipdate");
        BitSet set = new BitSet(reader.getRowCount(no1));
        int i = 0;
        while (reader.hasNext(no1)) {
            if (reader.nextValue(no1).toString().compareTo(args1) < 0) {
                set.set(i);
            }
            i++;
        }

        int no2 = reader.getColumnNO("l_commitdate");
        BitSet set2 = new BitSet(reader.getRowCount(no2));
        i = 0;
        while (reader.hasNext(no2)) {
            if (reader.nextValue(no2).toString().compareTo(args2) < 0) {
                set2.set(i);
            }
            i++;
        }
        set.and(set2);
        int mm = set.cardinality();

        int[] nn = new int[fs.size()];
        for (int j = 0; j < fs.size(); j++) {
            Field f = fs.get(j);
            int no = reader.getColumnNO(f.name());

            i = set.nextSetBit(0);
            while (i != -1) {
                reader.readValue(no, i);
                nn[j]++;
                if (++i >= set.length())
                    break;
                i = set.nextSetBit(i);
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("neci A2 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void neciRecordB2(String resultPath, String sPath, String args1, String args2, int max)
            throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan2.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);

        int no1 = reader.getColumnNO("l_shipdate");
        BitSet set = new BitSet(reader.getRowCount(no1));
        int i = 0;
        while (reader.hasNext(no1)) {
            if (reader.nextValue(no1).toString().compareTo(args1) < 0)
                set.set(i);
            i++;
        }

        int no2 = reader.getColumnNO("l_commitdate");
        i = set.nextSetBit(0);
        while (i != -1) {
            if (reader.readValue(no2, i).toString().compareTo(args2) >= 0)
                set.clear(i);
            if (++i >= set.length())
                break;
            i = set.nextSetBit(i);
        }

        int all = set.cardinality();
        int res = all;

        int sta = 0;
        while (max < all) {
            Record[] xxx = new Record[max];
            for (int x = 0; x < max; x++) {
                xxx[x] = new Record(s);
            }

            int[] nn = new int[fs.size()];
            for (int j = 0; j < fs.size(); j++) {
                Field f = fs.get(j);
                int no = reader.getColumnNO(f.name());
                int st = sta;
                int mm = 0;
                while (mm < max) {
                    i = set.nextSetBit(st);
                    xxx[nn[j]].put(j, reader.readValue(no, i));
                    nn[j]++;
                    mm++;
                    st = i + 1;
                }
            }
            all -= max;
            sta = i + 1;
        }

        if (all > 0) {
            Record[] xxx = new Record[all];
            for (int x = 0; x < all; x++) {
                xxx[x] = new Record(s);
            }

            int[] nn = new int[fs.size()];
            for (int j = 0; j < fs.size(); j++) {
                Field f = fs.get(j);
                int no = reader.getColumnNO(f.name());
                i = set.nextSetBit(sta);
                while (i != -1) {
                    xxx[nn[j]].put(j, reader.readValue(no, i));
                    nn[j]++;
                    if (++i >= set.length())
                        break;
                    i = set.nextSetBit(i);
                }
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("B2 neciRecord scan time: " + (end - start));
        System.out.println("res: " + res);
    }

    public static void neciScanB2(String resultPath, String sPath, String args1, String args2) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan2.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);

        int no1 = reader.getColumnNO("l_shipdate");
        BitSet set = new BitSet(reader.getRowCount(no1));
        int i = 0;
        while (reader.hasNext(no1)) {
            if (reader.nextValue(no1).toString().compareTo(args1) < 0) {
                set.set(i);
            }
            i++;
        }

        int no2 = reader.getColumnNO("l_commitdate");
        i = set.nextSetBit(0);
        while (i != -1) {
            if (reader.readValue(no2, i).toString().compareTo(args2) >= 0)
                set.clear(i);
            if (++i >= set.length())
                break;
            i = set.nextSetBit(i);
        }
        int mm = set.cardinality();

        int[] nn = new int[fs.size()];
        for (int j = 0; j < fs.size(); j++) {
            Field f = fs.get(j);
            int no = reader.getColumnNO(f.name());

            i = set.nextSetBit(0);
            while (i != -1) {
                reader.readValue(no, i);
                nn[j]++;
                if (++i >= set.length())
                    break;
                i = set.nextSetBit(i);
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("neci B2 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void neciRecordA3(String resultPath, String sPath, String args1, String args2, String args3, int max)
            throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan3.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);

        int no1 = reader.getColumnNO("l_shipdate");
        BitSet set = new BitSet(reader.getRowCount(no1));
        int i = 0;
        while (reader.hasNext(no1)) {
            if (reader.nextValue(no1).toString().compareTo(args1) < 0) {
                set.set(i);
            }
            i++;
        }

        int no2 = reader.getColumnNO("l_commitdate");
        BitSet set2 = new BitSet(reader.getRowCount(no2));
        i = 0;
        while (reader.hasNext(no2)) {
            if (reader.nextValue(no2).toString().compareTo(args2) < 0) {
                set2.set(i);
            }
            i++;
        }
        set.and(set2);

        int no3 = reader.getColumnNO("l_receiptdate");
        BitSet set3 = new BitSet(reader.getRowCount(no3));
        i = 0;
        while (reader.hasNext(no3)) {
            if (reader.nextValue(no3).toString().compareTo(args3) < 0) {
                set3.set(i);
            }
            i++;
        }
        set.and(set3);

        int all = set.cardinality();
        int res = all;

        int sta = 0;
        while (max < all) {
            Record[] xxx = new Record[max];
            for (int x = 0; x < max; x++) {
                xxx[x] = new Record(s);
            }

            int[] nn = new int[fs.size()];
            for (int j = 0; j < fs.size(); j++) {
                Field f = fs.get(j);
                int no = reader.getColumnNO(f.name());
                int st = sta;
                int mm = 0;
                while (mm < max) {
                    i = set.nextSetBit(st);
                    xxx[nn[j]].put(j, reader.readValue(no, i));
                    nn[j]++;
                    mm++;
                    st = i + 1;
                }
            }
            all -= max;
            sta = i + 1;
        }

        if (all > 0) {
            Record[] xxx = new Record[all];
            for (int x = 0; x < all; x++) {
                xxx[x] = new Record(s);
            }

            int[] nn = new int[fs.size()];
            for (int j = 0; j < fs.size(); j++) {
                Field f = fs.get(j);
                int no = reader.getColumnNO(f.name());
                i = set.nextSetBit(sta);
                while (i != -1) {
                    xxx[nn[j]].put(j, reader.readValue(no, i));
                    nn[j]++;
                    if (++i >= set.length())
                        break;
                    i = set.nextSetBit(i);
                }
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("A3 neciRecord scan time: " + (end - start));
        System.out.println("res: " + res);
    }

    public static void neciScanA3(String resultPath, String sPath, String args1, String args2, String args3)
            throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan3.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);

        int no1 = reader.getColumnNO("l_shipdate");
        BitSet set = new BitSet(reader.getRowCount(no1));
        int i = 0;
        while (reader.hasNext(no1)) {
            if (reader.nextValue(no1).toString().compareTo(args1) < 0) {
                set.set(i);
            }
            i++;
        }

        int no2 = reader.getColumnNO("l_commitdate");
        BitSet set2 = new BitSet(reader.getRowCount(no2));
        i = 0;
        while (reader.hasNext(no2)) {
            if (reader.nextValue(no2).toString().compareTo(args2) < 0) {
                set2.set(i);
            }
            i++;
        }
        set.and(set2);

        int no3 = reader.getColumnNO("l_receiptdate");
        BitSet set3 = new BitSet(reader.getRowCount(no3));
        i = 0;
        while (reader.hasNext(no3)) {
            if (reader.nextValue(no3).toString().compareTo(args3) < 0) {
                set3.set(i);
            }
            i++;
        }
        set.and(set3);

        int mm = set.cardinality();

        int[] nn = new int[fs.size()];
        for (int j = 0; j < fs.size(); j++) {
            Field f = fs.get(j);
            int no = reader.getColumnNO(f.name());

            i = set.nextSetBit(0);
            while (i != -1) {
                reader.readValue(no, i);
                nn[j]++;
                if (++i >= set.length())
                    break;
                i = set.nextSetBit(i);
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("neci A3 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void neciRecordB3(String resultPath, String sPath, String args1, String args2, String args3, int max)
            throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan3.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);

        int no1 = reader.getColumnNO("l_shipdate");
        BitSet set = new BitSet(reader.getRowCount(no1));
        int i = 0;
        while (reader.hasNext(no1)) {
            if (reader.nextValue(no1).toString().compareTo(args1) < 0) {
                set.set(i);
            }
            i++;
        }

        int no2 = reader.getColumnNO("l_commitdate");
        i = set.nextSetBit(0);
        while (i != -1) {
            if (reader.readValue(no2, i).toString().compareTo(args2) >= 0)
                set.clear(i);
            if (++i >= set.length())
                break;
            i = set.nextSetBit(i);
        }

        int no3 = reader.getColumnNO("l_receiptdate");
        i = set.nextSetBit(0);
        while (i != -1) {
            if (reader.readValue(no3, i).toString().compareTo(args3) >= 0)
                set.clear(i);
            if (++i >= set.length())
                break;
            i = set.nextSetBit(i);
        }

        int all = set.cardinality();
        int res = all;

        int sta = 0;
        while (max < all) {
            Record[] xxx = new Record[max];
            for (int x = 0; x < max; x++) {
                xxx[x] = new Record(s);
            }

            int[] nn = new int[fs.size()];
            for (int j = 0; j < fs.size(); j++) {
                Field f = fs.get(j);
                int no = reader.getColumnNO(f.name());
                int st = sta;
                int mm = 0;
                while (mm < max) {
                    i = set.nextSetBit(st);
                    xxx[nn[j]].put(j, reader.readValue(no, i));
                    nn[j]++;
                    mm++;
                    st = i + 1;
                }
            }
            all -= max;
            sta = i + 1;
        }

        if (all > 0) {
            Record[] xxx = new Record[all];
            for (int x = 0; x < all; x++) {
                xxx[x] = new Record(s);
            }

            int[] nn = new int[fs.size()];
            for (int j = 0; j < fs.size(); j++) {
                Field f = fs.get(j);
                int no = reader.getColumnNO(f.name());
                i = set.nextSetBit(sta);
                while (i != -1) {
                    xxx[nn[j]].put(j, reader.readValue(no, i));
                    nn[j]++;
                    if (++i >= set.length())
                        break;
                    i = set.nextSetBit(i);
                }
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("B3 neciRecord scan time: " + (end - start));
        System.out.println("res: " + res);
    }

    public static void neciScanB3(String resultPath, String sPath, String args1, String args2, String args3)
            throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan3.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);

        int no1 = reader.getColumnNO("l_shipdate");
        BitSet set = new BitSet(reader.getRowCount(no1));
        int i = 0;
        while (reader.hasNext(no1)) {
            if (reader.nextValue(no1).toString().compareTo(args1) < 0) {
                set.set(i);
            }
            i++;
        }

        int no2 = reader.getColumnNO("l_commitdate");
        i = set.nextSetBit(0);
        while (i != -1) {
            if (reader.readValue(no2, i).toString().compareTo(args2) >= 0)
                set.clear(i);
            if (++i >= set.length())
                break;
            i = set.nextSetBit(i);
        }

        int no3 = reader.getColumnNO("l_receiptdate");
        i = set.nextSetBit(0);
        while (i != -1) {
            if (reader.readValue(no3, i).toString().compareTo(args3) >= 0)
                set.clear(i);
            if (++i >= set.length())
                break;
            i = set.nextSetBit(i);
        }

        int mm = set.cardinality();

        int[] nn = new int[fs.size()];
        for (int j = 0; j < fs.size(); j++) {
            Field f = fs.get(j);
            int no = reader.getColumnNO(f.name());

            i = set.nextSetBit(0);
            while (i != -1) {
                reader.readValue(no, i);
                nn[j]++;
                if (++i >= set.length())
                    break;
                i = set.nextSetBit(i);
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("neci B3 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void avroScan1(String resultPath, String sPath, String args1) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan1.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int[] no = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            no[i] = reader.getColumnNO(fs.get(i).name());
        }
        int mm = 0;
        int n = reader.getColumnNO("l_shipdate");
        while (reader.hasNext(n)) {
            if (reader.nextValue(n).toString().compareTo(args1) < 0) {
                mm++;
            }
            for (int oo : no)
                reader.nextValue(oo);
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("avro A1 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void avroNo1(String resultPath, String sPath, String args1) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan1.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int[] no = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            no[i] = reader.getColumnNO(fs.get(i).name());
        }
        int mm = 0;
        int n = reader.getColumnNO("l_shipdate");
        while (reader.hasNext(n)) {
            if (reader.nextValue(n).toString().compareTo(args1) < 0) {
                mm++;
            }
            for (int oo : no)
                reader.skipValue(oo);
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("avro A1 no time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void avroScan2(String resultPath, String sPath, String args1, String args2) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan2.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int[] no = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            no[i] = reader.getColumnNO(fs.get(i).name());
        }
        int mm = 0;
        int n1 = reader.getColumnNO("l_shipdate");
        int n2 = reader.getColumnNO("l_commitdate");
        while (reader.hasNext(n1)) {
            String s1 = reader.nextValue(n1).toString();
            String s2 = reader.nextValue(n2).toString();
            if (s1.compareTo(args1) < 0 && s2.compareTo(args2) < 0) {
                mm++;
            }
            for (int oo : no)
                reader.nextValue(oo);
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("avro A2 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void avroScan3(String resultPath, String sPath, String args1, String args2, String args3)
            throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan3.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int[] no = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            no[i] = reader.getColumnNO(fs.get(i).name());
        }
        int mm = 0;
        int n1 = reader.getColumnNO("l_shipdate");
        int n2 = reader.getColumnNO("l_commitdate");
        int n3 = reader.getColumnNO("l_receiptdate");
        while (reader.hasNext(n1)) {
            String s1 = reader.nextValue(n1).toString();
            String s2 = reader.nextValue(n2).toString();
            String s3 = reader.nextValue(n3).toString();
            if (s1.toString().compareTo(args1) < 0 && s2.compareTo(args2) < 0 && s3.compareTo(args3) < 0) {
                mm++;
            }
            for (int oo : no)
                reader.nextValue(oo);
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("avro A3 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void avroRecordScan1(String resultPath, String sPath, String args1) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int mm = 0;
        reader.createSchema(s);
        while (reader.hasNext()) {
            Record r = reader.next();
            if (r.get("l_shipdate").toString().compareTo(args1) < 0)
                mm++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("avroRecord A1 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void avroRecordScan2(String resultPath, String sPath, String args1, String args2) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int mm = 0;
        reader.createSchema(s);
        while (reader.hasNext()) {
            Record r = reader.next();
            if (r.get("l_shipdate").toString().compareTo(args1) < 0
                    && r.get("l_commitdate").toString().compareTo(args2) < 0)
                mm++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("avroRecord A1 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void avroRecordScan3(String resultPath, String sPath, String args1, String args2, String args3)
            throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int mm = 0;
        reader.createSchema(s);
        while (reader.hasNext()) {
            Record r = reader.next();
            if (r.get("l_shipdate").toString().compareTo(args1) < 0
                    && r.get("l_commitdate").toString().compareTo(args2) < 0
                    && r.get("l_receiptdate").toString().compareTo(args3) < 0)
                mm++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("avroRecord A1 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void filterScan1(String resultPath, String sPath, String args1) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan1.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int[] no = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            no[i] = reader.getColumnNO(fs.get(i).name());
        }
        int mm = 0;
        int i = 0;
        int n = reader.getColumnNO("l_shipdate");
        while (reader.hasNext(n)) {
            if (reader.nextValue(n).toString().compareTo(args1) < 0) {
                mm++;
                for (int oo : no)
                    reader.readValue(oo, i);
            }
            i++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("filter A1 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void filterNo1(String resultPath, String sPath, String args1) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan1.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int[] no = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            no[i] = reader.getColumnNO(fs.get(i).name());
        }
        int mm = 0;
        int i = 0;
        int n = reader.getColumnNO("l_shipdate");
        while (reader.hasNext(n)) {
            if (reader.nextValue(n).toString().compareTo(args1) < 0) {
                mm++;
                for (int oo : no)
                    reader.skipValue(oo, i);
            }
            i++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("filter A1 no time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void filterScan2(String resultPath, String sPath, String args1, String args2) throws IOException {
        long start = System.currentTimeMillis();
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan2.avsc"));
        List<Field> fs = s.getFields();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int[] no = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            no[i] = reader.getColumnNO(fs.get(i).name());
        }
        int mm = 0;
        int i = 0;
        int n1 = reader.getColumnNO("l_shipdate");
        int n2 = reader.getColumnNO("l_commitdate");
        while (reader.hasNext(n1)) {
            String s1 = reader.nextValue(n1).toString();
            String s2 = reader.nextValue(n2).toString();
            if (s1.compareTo(args1) < 0 && s2.compareTo(args2) < 0) {
                mm++;
                for (int oo : no)
                    reader.readValue(oo, i);
            }
            i++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("filter A2 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void filterScan3(String resultPath, String sPath, String args1, String args2, String args3)
            throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan3.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int[] no = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            no[i] = reader.getColumnNO(fs.get(i).name());
        }
        int mm = 0;
        int i = 0;
        int n1 = reader.getColumnNO("l_shipdate");
        int n2 = reader.getColumnNO("l_commitdate");
        int n3 = reader.getColumnNO("l_receiptdate");
        while (reader.hasNext(n1)) {
            String s1 = reader.nextValue(n1).toString();
            String s2 = reader.nextValue(n2).toString();
            String s3 = reader.nextValue(n3).toString();
            if (s1.compareTo(args1) < 0 && s2.compareTo(args2) < 0 && s3.compareTo(args3) < 0) {
                mm++;
                for (int oo : no)
                    reader.readValue(oo, i);
            }
            i++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("filter A3 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void filterRecordScan1(String resultPath, String sPath, String args1) throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan1.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int mm = 0;
        int i = 0;
        reader.createSchema(s);
        int n1 = reader.getColumnNO("l_shipdate");
        while (reader.hasNext(n1)) {
            if (reader.nextValue(n1).toString().compareTo(args1) < 0) {
                mm++;
                Record m = (Record) reader.search(i);
            }
            i++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("filterRecord A1 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void filterRecordScan2(String resultPath, String sPath, String args1, String args2)
            throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan2.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int mm = 0;
        int i = 0;
        reader.createSchema(s);
        int n1 = reader.getColumnNO("l_shipdate");
        int n2 = reader.getColumnNO("l_commitdate");
        while (reader.hasNext(n1)) {
            String s1 = reader.nextValue(n1).toString();
            String s2 = reader.nextValue(n2).toString();
            if (s1.compareTo(args1) < 0 && s2.compareTo(args2) < 0) {
                mm++;
                reader.search(i);
            }
            i++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("filterRecord A2 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void filterRecordScan3(String resultPath, String sPath, String args1, String args2, String args3)
            throws IOException {
        Schema s = new Schema.Parser().parse(new File(sPath + "l_scan3.avsc"));
        List<Field> fs = s.getFields();
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(result);
        int mm = 0;
        int i = 0;
        reader.createSchema(s);
        int n1 = reader.getColumnNO("l_shipdate");
        int n2 = reader.getColumnNO("l_commitdate");
        int n3 = reader.getColumnNO("l_receiptdate");
        while (reader.hasNext(n1)) {
            String s1 = reader.nextValue(n1).toString();
            String s2 = reader.nextValue(n2).toString();
            String s3 = reader.nextValue(n3).toString();
            if (s1.compareTo(args1) < 0 && s2.compareTo(args2) < 0 && s3.compareTo(args3) < 0) {
                mm++;
                reader.search(i);
            }
            i++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("filterRecord A3 scan time: " + (end - start));
        System.out.println("mm: " + mm);
    }

    public static void main(String[] args) throws IOException {
        int free = Integer.parseInt(args[0]);
        int mul = Integer.parseInt(args[11]);
        String lPath = args[1];
        String resultPath = args[2];
        if (!new File(resultPath).exists())
            new File(resultPath).mkdirs();
        String tmpPath = args[3];
        if (!new File(tmpPath).exists())
            new File(tmpPath).mkdirs();
        String sPath = args[4];

        if (args[5].equals("load")) {
            Schema s = new Schema.Parser().parse(new File(sPath));
            List<Field> fs = s.getFields();
            loadLineitem(DEFAULT_BLOCK_KBYTES, free, mul, lPath, resultPath, sPath, fs, s);
        } else if (args[5].equals("neciRecord")) {
            System.out.println("##########" + args[6]);
            int max = Integer.parseInt(args[7]);
            if (args[6].equals("A1"))
                neciRecordA1(resultPath, sPath, args[8], max);
            else if (args[6].equals("A2"))
                neciRecordA2(resultPath, sPath, args[8], args[9], max);
            else if (args[6].equals("A3"))
                neciRecordA3(resultPath, sPath, args[8], args[9], args[10], max);
            else if (args[6].equals("B2"))
                neciRecordB2(resultPath, sPath, args[8], args[9], max);
            else if (args[6].equals("B3"))
                neciRecordB3(resultPath, sPath, args[8], args[9], args[10], max);
        } else if (args[5].equals("neciScan")) {
            System.out.println("##########" + args[6]);
            if (args[6].equals("no"))
                neciNoA1(resultPath, sPath, args[7]);
            if (args[6].equals("A1"))
                neciScanA1(resultPath, sPath, args[7]);
            else if (args[6].equals("A2"))
                neciScanA2(resultPath, sPath, args[7], args[8]);
            else if (args[6].equals("A3"))
                neciScanA3(resultPath, sPath, args[7], args[8], args[9]);
            else if (args[6].equals("B2"))
                neciScanB2(resultPath, sPath, args[7], args[8]);
            else if (args[6].equals("B3"))
                neciScanB3(resultPath, sPath, args[7], args[8], args[9]);
        } else if (args[5].equals("avro")) {
            System.out.println("##########" + args[6]);
            if (args[6].equals("no"))
                avroNo1(resultPath, sPath, args[7]);
            if (args[6].equals("A1"))
                avroScan1(resultPath, sPath, args[7]);
            else if (args[6].equals("A2"))
                avroScan2(resultPath, sPath, args[7], args[8]);
            else if (args[6].equals("A3"))
                avroScan3(resultPath, sPath, args[7], args[8], args[9]);
        } else if (args[5].equals("avroFilter")) {
            System.out.println("##########" + args[6]);
            if (args[6].equals("no"))
                filterNo1(resultPath, sPath, args[7]);
            if (args[6].equals("A1"))
                filterScan1(resultPath, sPath, args[7]);
            else if (args[6].equals("A2"))
                filterScan2(resultPath, sPath, args[7], args[8]);
            else if (args[6].equals("A3"))
                filterScan3(resultPath, sPath, args[7], args[8], args[9]);
        } else if (args[5].equals("avroRecord")) {
            System.out.println("##########" + args[6]);
            if (args[6].equals("A1"))
                avroRecordScan1(resultPath, sPath, args[7]);
            else if (args[6].equals("A2"))
                avroRecordScan2(resultPath, sPath, args[7], args[8]);
            else if (args[6].equals("A3"))
                avroRecordScan3(resultPath, sPath, args[7], args[8], args[9]);
        } else if (args[5].equals("filterRecord")) {
            System.out.println("##########" + args[6]);
            if (args[6].equals("A1"))
                filterRecordScan1(resultPath, sPath, args[7]);
            else if (args[6].equals("A2"))
                filterRecordScan2(resultPath, sPath, args[7], args[8]);
            else if (args[6].equals("A3"))
                filterRecordScan3(resultPath, sPath, args[7], args[8], args[9]);
        }
    }
}

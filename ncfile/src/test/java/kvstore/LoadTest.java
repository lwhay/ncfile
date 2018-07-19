package kvstore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import columnar.BlockManager;
import neci.ncfile.KeyofBTree;
import neci.ncfile.NestManager;
import neci.ncfile.NestSchema;
import neci.ncfile.base.Schema;
import neci.ncfile.base.Schema.Field;
import neci.ncfile.generic.GenericData.Record;

/**
 * Modified by michael on 2018/7/16.
 * Default parameters:
 * COL ./src/resources/tpch/ ./src/resources/kvschema/ ./src/storage/ ./src/resources/tpch/ 1000 1000 10000 load
 */

public class LoadTest {
    private static final int DEFAULT_BLOCK_KBYTES = 32;
    static Schema lSchema;
    static Schema oSchema;
    static Schema cSchema;
    static NestManager load;

    public static void loadTest(NestManager load) throws IOException {
        load.load();
    }

    public static void scanTest(NestManager load, String path) throws IOException {
        System.out.println("----------------SCAN--------------------");
        Schema lkS = new Schema.Parser().parse(
                "{\"type\":\"record\", \"name\":\"Lineitem\", \"fields\":[{\"name\":\"l_orderkey\", \"type\":\"long\"}, {\"name\":\"l_linenumber\", \"type\":\"int\"}]}");
        File c = new File(path + "scanlineitem.tbl");
        BufferedReader reader = new BufferedReader(new FileReader(c));
        String line;
        List<Integer> l1 = new ArrayList<Integer>();
        List<Integer> l2 = new ArrayList<Integer>();
        while ((line = reader.readLine()) != null) {
            //String[] tmp = line.split("\\|", 2);
            String[] tmp = line.split("\\|", 5);
            l1.add(Integer.parseInt(tmp[0]));
            l2.add(Integer.parseInt(tmp[3]));
        }
        reader.close();
        for (int i = 0; i < l1.size(); i++) {
            //Record data = new Record(oSchema);
            //data.put(0, ck);
            Record data = new Record(lkS);
            data.put(0, l1.get(i));
            data.put(1, l2.get(i));
            Record liData = new Record(lSchema);
            liData.put(0, l1.get(i));
            liData.put(3, l2.get(i));
            int lvl = load.getLevel(data);
            KeyofBTree key = load.getUpperKey(liData);
            Record ksl = load.search(data, lkS);
            System.out.print(load.exists(ksl) + "-");
            if (ksl == null) {
                System.out.print(lvl + ":" + key.getKey()[0] + "<" + l1.get(i) + "," + l2.get(i) + ">" + "\t");
            } else {
                System.out.print(
                        lvl + ":" + key.getKey()[0] + "<" + l1.get(i) + "," + l2.get(i) + ">" + ksl.get(0) + "\t");
            }
            if (i % 10 != 0) {
                System.out.println();
                break;
            }
        }
        System.out.println();
        System.out.println("----------------SCAN--------------------");
    }

    public static void deleteTest(NestManager load, String path) throws IOException {
        System.out.println("-----------------DELETE------------------");
        File c = new File(path + "deletelineitem.tbl");
        BufferedReader reader = new BufferedReader(new FileReader(c));
        String line;
        List<Integer> l1 = new ArrayList<Integer>();
        List<Integer> l2 = new ArrayList<Integer>();
        while ((line = reader.readLine()) != null) {
            //String[] tmp = line.split("\\|", 2);
            String[] tmp = line.split("\\|", 5);
            l1.add(Integer.parseInt(tmp[0]));
            l2.add(Integer.parseInt(tmp[3]));
        }
        reader.close();
        //        Collections.shuffle(cks);
        //        reader = new BufferedReader(new FileReader(c));
        //        BufferedWriter writer = new BufferedWriter(new FileWriter(path + "tmp"));
        //        int i = 0;
        //        while ((line = reader.readLine()) != null) {
        //            String[] tmp = line.split("\\|", 2);
        //            writer.write(cks.get(i) + "|" + tmp[1]);
        //            writer.newLine();
        //            i++;
        //        }
        //        writer.flush();
        //        writer.close();
        //        c.delete();
        //        new File(path + "tmp").renameTo(c);
        //reader.close();
        for (int i = 0; i < l1.size(); i++) {
            //Record data = new Record(oSchema);
            //data.put(0, ck);
            Record data = new Record(lSchema);
            data.put(0, l1.get(i));
            data.put(3, l2.get(i));
            load.delete(data);
        }
        load.merge();
        System.out.println("-----------------DELETE------------------");
    }

    public static void insertTest(NestManager load, String path, boolean isCOL) throws IOException {
        System.out.println("-----------------INSERT------------------");
        File c = new File(path + "xab");
        BufferedReader reader = new BufferedReader(new FileReader(c));
        String line;
        List<Field> fs = oSchema.getFields();
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record data = new Record(oSchema);
            for (int i = 0; i < fs.size(); i++) {
                switch (fs.get(i).schema().getType()) {
                    case INT:
                        data.put(i, Integer.parseInt(tmp[i]));
                        break;
                    case LONG:
                        data.put(i, Long.parseLong(tmp[i]));
                        break;
                    case FLOAT:
                        data.put(i, Float.parseFloat(tmp[i]));
                        break;
                    case DOUBLE:
                        data.put(i, Double.parseDouble(tmp[i]));
                        break;
                    case BYTES:
                        data.put(i, ByteBuffer.wrap(tmp[i].getBytes()));
                        break;
                    default:
                        data.put(i, tmp[i]);
                }
            }
            load.insert(data);
        }
        reader.close();
        //        File o;
        //        if (isCOL)
        //            o = new File(path + "orders.tbl");
        //        else
        //            o = new File(path + "partsupp.tbl");
        //        reader = new BufferedReader(new FileReader(o));
        //        fs = oSchema.getFields();
        //        while ((line = reader.readLine()) != null) {
        //            String[] tmp = line.split("\\|");
        //            Record data = new Record(oSchema);
        //            for (int i = 0; i < fs.size(); i++) {
        //                switch (fs.get(i).schema().getType()) {
        //                    case INT:
        //                        data.put(i, Integer.parseInt(tmp[i]));
        //                        break;
        //                    case LONG:
        //                        data.put(i, Long.parseLong(tmp[i]));
        //                        break;
        //                    case FLOAT:
        //                        data.put(i, Float.parseFloat(tmp[i]));
        //                        break;
        //                    case DOUBLE:
        //                        data.put(i, Double.parseDouble(tmp[i]));
        //                        break;
        //                    case BYTES:
        //                        data.put(i, tmp[i].getBytes());
        //                        break;
        //                    default:
        //                        data.put(i, tmp[i]);
        //                }
        //            }
        //            load.insert(data);
        //        }
        //        reader.close();

        File l = new File(path + "lineitem.tbl");
        reader = new BufferedReader(new FileReader(l));
        fs = lSchema.getFields();
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record data = new Record(lSchema);
            for (int i = 0; i < fs.size(); i++) {
                switch (fs.get(i).schema().getType()) {
                    case INT:
                        data.put(i, Integer.parseInt(tmp[i]));
                        break;
                    case LONG:
                        data.put(i, Long.parseLong(tmp[i]));
                        break;
                    case FLOAT:
                        data.put(i, Float.parseFloat(tmp[i]));
                        break;
                    case DOUBLE:
                        data.put(i, Double.parseDouble(tmp[i]));
                        break;
                    case BYTES:
                        data.put(i, tmp[i].getBytes());
                        break;
                    default:
                        data.put(i, tmp[i]);
                }
            }
            load.insert(data);

        }
        reader.close();
        load.merge();
        System.out.println("-----------------INSERT------------------");
    }

    public static void updateTest(NestManager load, String path) throws IOException {
        System.out.println("-----------------UPDATE------------------");
        File c = new File(path + "x1");
        String line;
        if (c.exists()) {
            BufferedReader reader = new BufferedReader(new FileReader(c));

            while ((line = reader.readLine()) != null) {
                String[] tmp = line.split("\\|", 2);
                Record data = new Record(cSchema);
                data.put(0, Integer.parseInt(tmp[0]));
                load.update(data);
            }
            reader.close();
        }

        File o = new File(path + "x2");
        if (o.exists()) {
            BufferedReader reader = new BufferedReader(new FileReader(o));
            while ((line = reader.readLine()) != null) {
                String[] tmp = line.split("\\|", 2);
                Record data = new Record(oSchema);
                data.put(0, Integer.parseInt(tmp[0]));
                load.update(data);
            }
            reader.close();
        }

        File l = new File(path + "x3");
        if (l.exists()) {
            BufferedReader reader = new BufferedReader(new FileReader(l));
            while ((line = reader.readLine()) != null) {
                String[] tmp = line.split("\\|", 5);
                Record data = new Record(lSchema);
                data.put(0, Integer.parseInt(tmp[0]));
                data.put(3, Integer.parseInt(tmp[3]));
                load.update(data);
            }
            reader.close();
        }
        load.merge();
        //        Collections.shuffle(cks);
        //        reader = new BufferedReader(new FileReader(c));
        //        BufferedWriter writer = new BufferedWriter(new FileWriter(path + "tmp"));
        //        int i = 0;
        //        while ((line = reader.readLine()) != null) {
        //            String[] tmp = line.split("\\|", 2);
        //            writer.write(cks.get(i) + "|" + tmp[1]);
        //            writer.newLine();
        //            i++;
        //        }
        //        writer.flush();
        //        writer.close();
        //        c.delete();
        //        new File(path + "tmp").renameTo(c);
        System.out.println("-----------------UPDATE------------------");
    }

    public static void offsetTest(NestManager load, String path) throws IOException {
        System.out.println("-----------------OFFSET------------------");
        File c = new File(path + "customer.tbl");
        BufferedReader reader = new BufferedReader(new FileReader(c));
        int t = 0, f = 0;
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|", 2);
            Record data = new Record(cSchema);
            data.put(0, Integer.parseInt(tmp[0]));
            if (load.exists(data))
                t++;
            else {
                f++;
                System.out.println("ck:\t" + tmp[0]);
            }
        }
        reader.close();
        System.out.println("customer\nexists: " + t + "\tnot exists: " + f);

        File o = new File(path + "orders.tbl");
        reader = new BufferedReader(new FileReader(o));
        t = 0;
        f = 0;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|", 2);
            Record data = new Record(oSchema);
            data.put(0, Integer.parseInt(tmp[0]));
            if (load.exists(data))
                t++;
            else {
                f++;
                System.out.println("ok:\t" + tmp[0]);
            }
        }
        reader.close();
        System.out.println("orders\nexists: " + t + "\tnot exists: " + f);

        File l = new File(path + "lineitem.tbl");
        reader = new BufferedReader(new FileReader(l));
        t = 0;
        f = 0;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|", 5);
            Record data = new Record(lSchema);
            data.put(0, Integer.parseInt(tmp[0]));
            data.put(3, Integer.parseInt(tmp[3]));
            if (load.exists(data))
                t++;
            else {
                f++;
                System.out.println("lk:\t" + tmp[0] + "|" + tmp[3]);
            }
        }
        reader.close();
        System.out.println("lineitem\nexists: " + t + "\tnot exists: " + f);
        System.out.println("-----------------OFFSET------------------");
    }

    public static void backTest(NestManager load, String path) throws IOException {
        System.out.println("-----------------BACK------------------");
        int[] fields1 = new int[] { 1 };
        int[] fields2 = new int[] { 0 };

        File o = new File(path + "orders.tbl");
        BufferedReader reader = new BufferedReader(new FileReader(o));
        int t = 0;
        int f = 0;
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|", 3);
            Record data = new Record(oSchema);
            data.put(0, Integer.parseInt(tmp[0]));
            data.put(1, Integer.parseInt(tmp[1]));
            KeyofBTree up = load.getUpperKey(data);
            if (up.equals(new KeyofBTree(data, fields1)))
                t++;
            else {
                f++;
                System.out.println("ok:" + tmp[0] + "\nck:" + tmp[1] + " VS " + up);
            }
        }
        reader.close();
        System.out.println("orders\nexists: " + t + "\tnot exists: " + f);

        //        File l = new File(path + "lineitem.tbl");
        //        reader = new BufferedReader(new FileReader(l));
        //        t = 0;
        //        f = 0;
        //        while ((line = reader.readLine()) != null) {
        //            String[] tmp = line.split("\\|", 5);
        //            Record data = new Record(lSchema);
        //            data.put(0, Integer.parseInt(tmp[0]));
        //            data.put(3, Integer.parseInt(tmp[3]));
        //            CombKey up = load.getUpperKey(data);
        //            if (up.equals(new CombKey(data, fields2)))
        //                t++;
        //            else {
        //                f++;
        //                System.out.println("lk:" + tmp[0] + "|" + tmp[3] + "\nok:" + tmp[0] + " VS " + up.get());
        //            }
        //        }
        //        reader.close();
        //        System.out.println("lineitem\nexists: " + t + "\tnot exists: " + f);
        System.out.println("-----------------BACK------------------");
    }

    public static void upsertTest(NestManager load, String path) throws IOException {
        System.out.println("-----------------UPSERT------------------");
        File c = new File(path + "xab");
        BufferedReader reader = new BufferedReader(new FileReader(c));
        String line;
        List<Field> fs = cSchema.getFields();
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record data = new Record(cSchema);
            for (int i = 0; i < fs.size(); i++) {
                switch (fs.get(i).schema().getType()) {
                    case INT:
                        data.put(i, Integer.parseInt(tmp[i]));
                        break;
                    case LONG:
                        data.put(i, Long.parseLong(tmp[i]));
                        break;
                    case FLOAT:
                        data.put(i, Float.parseFloat(tmp[i]));
                        break;
                    case DOUBLE:
                        data.put(i, Double.parseDouble(tmp[i]));
                        break;
                    default:
                        data.put(i, tmp[i]);
                }
            }
            load.insert(data);
        }
        reader.close();

        File o = new File(path + "orders.tbl");
        reader = new BufferedReader(new FileReader(o));
        fs = oSchema.getFields();
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record data = new Record(oSchema);
            for (int i = 0; i < fs.size(); i++) {
                switch (fs.get(i).schema().getType()) {
                    case INT:
                        data.put(i, Integer.parseInt(tmp[i]));
                        break;
                    case LONG:
                        data.put(i, Long.parseLong(tmp[i]));
                        break;
                    case FLOAT:
                        data.put(i, Float.parseFloat(tmp[i]));
                        break;
                    case DOUBLE:
                        data.put(i, Double.parseDouble(tmp[i]));
                        break;
                    default:
                        data.put(i, tmp[i]);
                }
            }
            load.upsert(data);
        }
        reader.close();

        File l = new File(path + "lineitem.tbl");
        reader = new BufferedReader(new FileReader(l));
        fs = lSchema.getFields();
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record data = new Record(lSchema);
            for (int i = 0; i < fs.size(); i++) {
                switch (fs.get(i).schema().getType()) {
                    case INT:
                        data.put(i, Integer.parseInt(tmp[i]));
                        break;
                    case LONG:
                        data.put(i, Long.parseLong(tmp[i]));
                        break;
                    case FLOAT:
                        data.put(i, Float.parseFloat(tmp[i]));
                        break;
                    case DOUBLE:
                        data.put(i, Double.parseDouble(tmp[i]));
                        break;
                    default:
                        data.put(i, tmp[i]);
                }
            }
            load.upsert(data);
        }
        reader.close();
        load.merge();
        System.out.println("-----------------UPSERT------------------");
    }

    public static void searchTest(NestManager load, File file) throws IOException {
        Schema lkS = new Schema.Parser().parse(
                "{\"type\":\"record\", \"name\":\"Lineitem\", \"fields\":[{\"name\":\"l_orderkey\", \"type\":\"long\"}, {\"name\":\"l_linenumber\", \"type\":\"int\"}]}");
        Schema vS = new Schema.Parser().parse(
                "{\"type\":\"record\", \"name\":\"Lineitem\", \"fields\":[{\"name\":\"l_orderkey\", \"type\":\"long\"}, {\"name\":\"l_linenumber\", \"type\":\"int\"}, {\"name\":\"l_shipmode\", \"type\":\"string\"}, {\"name\":\"l_comment\", \"type\":\"string\"}]}");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        int t = 0, f = 0;
        String line;
        long start = System.currentTimeMillis();
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|", 5);
            Record key = new Record(lkS);
            key.put(0, Long.parseLong(tmp[0]));
            key.put(1, Integer.parseInt(tmp[3]));
            if (load.search(key, vS) != null) {
                t++;
            } else {
                f++;
            }
        }
        long end = System.currentTimeMillis();
        reader.close();
        System.out.println("result count\ntrue:" + t + "\tfalse:" + f);
        System.out.println("read + search time:" + (end - start));
    }

    public static void colTest(String filePath, String schemaPath, String toPath, String opPath, int max, int free,
            int mul, String arg) throws IOException {
        lSchema = new Schema.Parser().parse(new File(schemaPath + "lineitem.avsc"));
        oSchema = new Schema.Parser().parse(new File(schemaPath + "orders.avsc"));
        cSchema = new Schema.Parser().parse(new File(schemaPath + "customer.avsc"));

        NestSchema lNS = new NestSchema(lSchema, new int[] { 0, 3 }, new int[] { 0 });
        NestSchema oNS = new NestSchema(oSchema, new int[] { 0 }, new int[] { 1 });
        NestSchema cNS = new NestSchema(cSchema, new int[] { 0 });

        cNS.setPrFile(new File(filePath + "customer.tbl"));
        oNS.setPrFile(new File(filePath + "orders.tbl"));
        lNS.setPrFile(new File(filePath + "lineitem.tbl"));

        cNS.setPath(toPath + "customer/");
        oNS.setPath(toPath + "orders/");
        lNS.setPath(toPath + "lineitem/");

        cNS.setBloomFile(new File(toPath + "cBloom"));
        oNS.setBloomFile(new File(toPath + "oBloom"));
        lNS.setBloomFile(new File(toPath + "lBloom"));

        oNS.setBTreeFile(new File(toPath + "ock.db"));
        lNS.setBTreeFile(new File(toPath + "lok.db"));

        load = new NestManager(new NestSchema[] { cNS, oNS, lNS }, toPath + "tmp/", toPath + "result/", free, mul,
                new BlockManager(DEFAULT_BLOCK_KBYTES));
        load.setMax(max);

        if (arg.equals("load")) {
            loadTest(load);
        } else if (arg.equals("delete")) {
            load.openTree();
            deleteTest(load, opPath);
        } else if (arg.equals("insert")) {
            load.openTree();
            insertTest(load, opPath, true);
        } else if (arg.equals("offset")) {
            load.openTree();
            offsetTest(load, opPath);
        } else if (arg.equals("back")) {
            load.openTree();
            backTest(load, opPath);
        } else if (arg.equals("scan")) {
            load.openTree();
            scanTest(load, opPath);
        }
    }

    public static void ppslTest(String filePath, String schemaPath, String toPath, String opPath, int max, int free,
            int mul, String arg) throws IOException {
        lSchema = new Schema.Parser().parse(new File(schemaPath + "lineitem.avsc"));
        oSchema = new Schema.Parser().parse(new File(schemaPath + "partsupp.avsc"));
        cSchema = new Schema.Parser().parse(new File(schemaPath + "part.avsc"));

        NestSchema lNS = new NestSchema(lSchema, new int[] { 1, 2, 0 }, new int[] { 1, 2 });
        NestSchema oNS = new NestSchema(oSchema, new int[] { 0, 1 }, new int[] { 0 });
        NestSchema cNS = new NestSchema(cSchema, new int[] { 0 });

        cNS.setPrFile(new File(filePath + "part.tbl"));
        oNS.setPrFile(new File(filePath + "partsupp.tbl"));
        lNS.setPrFile(new File(filePath + "lineitem.tbl"));

        cNS.setPath(toPath + "part/");
        oNS.setPath(toPath + "partsupp/");
        lNS.setPath(toPath + "lineitem/");

        cNS.setBloomFile(new File(toPath + "pBloom"));
        oNS.setBloomFile(new File(toPath + "psBloom"));
        lNS.setBloomFile(new File(toPath + "lBloom"));

        oNS.setBTreeFile(new File(toPath + "pspk.db"));
        lNS.setBTreeFile(new File(toPath + "lpsk.db"));

        load = new NestManager(new NestSchema[] { cNS, oNS, lNS }, toPath + "tmp/", toPath + "result/", free, mul,
                new BlockManager(DEFAULT_BLOCK_KBYTES));
        load.setMax(max);
        load.openTree();
        if (arg.equals("load")) {
            loadTest(load);
        } else if (arg.equals("insert")) {
            deleteTest(load, opPath);
            //            load.create();
            insertTest(load, opPath, false);
        } else {
            deleteTest(load, opPath);
            //            load.create();
            upsertTest(load, opPath);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 9) {
            System.out.println(
                    "Comand COL|PPS dataPath schemaPath targetPath tmpPath maxTopLevelNumBudget remainingMemoryBudget gcPerc commandType");
            System.exit(-1);
        }
        String filePath = args[1];
        String schemaPath = args[2];
        String toPath = args[3];
        String opPath = args[4];
        int max = Integer.parseInt(args[5]);
        int free = Integer.parseInt(args[6]);
        int mul = Integer.parseInt(args[7]);
        String arg = args[8];
        if (arg.equals("load")) {
            NestManager.shDelete(toPath);
        }
        if (args[0].equals("COL"))
            colTest(filePath, schemaPath, toPath, opPath, max, free, mul, arg);
        else
            ppslTest(filePath, schemaPath, toPath, opPath, max, free, mul, arg);
    }
}

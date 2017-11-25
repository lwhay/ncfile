package neci.avro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;

import neci.ncfile.NestManager;
import neci.ncfile.NestSchema;

public class TwoNestTest {
    public static void main(String[] args) throws IOException {
        Schema oSchema = new Schema.Parser().parse(new File("/home/ly/schemas/orders.avsc"));
        Schema lSchema = new Schema.Parser().parse(new File("/home/ly/schemas/lineitem.avsc"));
        Schema cSchema = new Schema.Parser().parse(new File("/home/ly/schemas/customer.avsc"));
        NestSchema cNS = new NestSchema(cSchema, new int[] { 0 });
        NestSchema oNS = new NestSchema(oSchema, new int[] { 0 }, new int[] { 1 });
        NestSchema lNS = new NestSchema(lSchema, new int[] { 0, 3 }, new int[] { 0 });
        cNS.setPrFile(new File("/home/ly/customer.tbl"));
        oNS.setPrFile(new File("/home/ly/orders.tbl"));
        lNS.setPrFile(new File("/home/ly/lineitem.tbl"));
        cNS.setPath("/home/ly/test/customer/");
        oNS.setPath("/home/ly/test/orders/");
        lNS.setPath("/home/ly/test/lineitem/");
        cNS.setBloomFile(new File("/home/ly/test/cBloom"));
        oNS.setBloomFile(new File("/home/ly/test/oBloom"));
        lNS.setBloomFile(new File("/home/ly/test/lBloom"));
        oNS.setBTreeFile(new File("/home/ly/test/ock.db"));
        NestManager load = new NestManager(new NestSchema[] { cNS, oNS, lNS }, "/home/ly/test/tmp/",
                "/home/ly/test/result/", 150, 40);
        //        load.load();
        //        load.create();

        //        List<Integer> cks = new ArrayList<Integer>();
        //        BufferedReader reader = new BufferedReader(new FileReader(new File("/home/ly/xab")));
        //        String line;
        //        while ((line = reader.readLine()) != null) {
        //            String[] tmp = line.split("\\|", 2);
        //            cks.add(Integer.parseInt(tmp[0]));
        //        }
        //        Collections.shuffle(cks);
        //        for (int ck : cks) {
        //            Record data = new Record(cSchema);
        //            data.put(0, ck);
        //            load.delete(data);
        //        }
        //        load.merge();
        //
        //        load.create();
        //        reader = new BufferedReader(new FileReader(new File("/home/ly/xab")));
        //        List<Field> fs = cSchema.getFields();
        //        while ((line = reader.readLine()) != null) {
        //            String[] tmp = line.split("\\|");
        //            Record data = new Record(cSchema);
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
        //                    default:
        //                        data.put(i, tmp[i]);
        //                }
        //            }
        //            load.insert(data);
        //        }
        //        reader.close();
        BufferedReader reader = new BufferedReader(new FileReader(new File("/home/ly/dbgen/dbgen/orders.tbl")));
        List<Field> fs = oSchema.getFields();
        String line;
        for (int m = 0; m < 10; m++) {
            String[] tmp = reader.readLine().split("\\|");
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
                        data.put(i, tmp[i].getBytes());
                        break;
                    default:
                        data.put(i, tmp[i]);
                }
            }
            data.put(1, (Integer.parseInt(tmp[1]) + 1) % 1500);
            load.update(data);
        }
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
                        data.put(i, tmp[i].getBytes());
                        break;
                    default:
                        data.put(i, tmp[i]);
                }
            }
            load.update(data);
        }

        load.merge();

        //    Schema lkS = new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"Lineitem\", \"fields\":[{\"name\":\"l_orderkey\", \"type\":\"long\"}, {\"name\":\"l_linenumber\", \"type\":\"int\"}]}");
        //    Schema vS = new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"Lineitem\", \"fields\":[{\"name\":\"l_orderkey\", \"type\":\"long\"}, {\"name\":\"l_linenumber\", \"type\":\"int\"}, {\"name\":\"l_shipmode\", \"type\":\"string\"}, {\"name\":\"l_comment\", \"type\":\"string\"}]}");
        //    BufferedReader reader = new BufferedReader(new FileReader(new File("/home/ly/dbgen/dbgen/lineitem.tbl")));
        //    int t = 0, f = 0;
        //    String line;
        //    while((line = reader.readLine()) != null){
        //      String[] tmp = line.split("\\|", 5);
        //      Record key = new Record(lkS);
        //      key.put(0, Long.parseLong(tmp[0]));
        //      key.put(1, Integer.parseInt(tmp[3]));
        //      if(load.search(key, vS)){
        //        t++;
        //      }else{
        //        f++;
        //      }
        //    }
        //    reader.close();
        //    System.out.println("result count\ntrue:" + t + "\tfalse:" + f);

        //        Schema lS = new Schema.Parser().parse(new File("/home/ly/schemas/orders.avsc"));
        //        Record r = new Record(lS);
        //        long[] oks = { 1, 2, 2, 2, 2, 10, 12, 35999879, 35999904, 35999936, 35999974, 36000000 };
        //        int[] lcks = { 370, 800, 781, 800, 1234, 7, 1, 5, 1, 1, 4, 5 };
        //        for (int i = 0; i < 12; i++) {
        //            r.put(0, oks[i]);
        //            r.put(1, lcks[i]);
        //            load.update(r);
        //        }

        //    Params pa = new Params(new File("/home/ly/test/result/result.trv"));
        //    pa.setSchema(cNS.getNestedSchema());
        //    InsertAvroColumnReader<Record> reader = new InsertAvroColumnReader<Record>(pa);
        //    int l = 0;
        //    while(reader.hasNext()){
        //      Record r = reader.next();
        //      l++;
        //    }
        //    System.out.println("line:" + l);
        //    reader.close();
    }
}

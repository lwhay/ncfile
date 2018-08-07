/**
 * 
 */
package neci.single.tpch;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;

/**
 * @author Michael
 *
 */
public class TrevTPCH {
    private static final boolean comp = true;

    public static void Q01_FilteringScan(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema schema = new Schema.Parser().parse(new File(args[1]));
        long start = System.currentTimeMillis();
        Params param = new Params(file);
        param.setSchema(schema);
        AvroColumnReader<Record> reader = new AvroColumnReader<Record>(param);

        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        int offsetOrders = schema.getFields().size() - 1;
        Schema orderSchema = schema.getFields().get(schema.getFields().size() - 1).schema().getElementType();
        int offsetLines = orderSchema.getFields().size() - 1;
        Map<ByteBuffer, Map<ByteBuffer, float[]>> values = new HashMap<>();
        int valuecount = 0;
        while (reader.hasNext()) {
            Record r = reader.next();
            if (!comp) {
                continue;
            }
            List<Record> orders = (List<Record>) r.get(offsetOrders);
            for (Record order : orders) {
                List<Record> lines = (List<Record>) order.get(offsetLines);
                for (Record line : lines) {
                    String sdate = line.get("l_shipdate").toString();
                    if (sdate.compareTo("1998-09-01") > 0) {
                        continue;
                    }
                    ByteBuffer rf = (ByteBuffer) line.get("l_returnflag");
                    ByteBuffer ls = (ByteBuffer) line.get("l_linestatus");
                    if (!values.containsKey(rf)) {
                        values.put(rf, new HashMap<>());
                    }
                    if (!values.get(rf).containsKey(ls)) {
                        values.get(rf).put(ls, new float[6]);
                        valuecount++;
                    }
                    float[] aggs = values.get(rf).get(ls);
                    aggs[0] += (float) line.get("l_quantity");
                    float ep = (float) line.get("l_extendedprice");
                    float dc = (float) line.get("l_discount");
                    float tx = (float) line.get("l_tax");
                    aggs[1] += ep;
                    aggs[2] += dc;
                    aggs[3] += ep * (1 - dc);
                    aggs[4] += ep * (1 - dc) * (1 + tx);
                    aggs[5]++;
                    values.get(rf).put(ls, aggs);
                    count++;
                }
            }
            //result += (float) r.get(1) * (float) r.get(2);
        }
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(valuecount);
        System.out.println(count);
        System.out.println("Trev time: " + (end - start) + " result: " + result);
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }

    public static void Q03_FilteringScan(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema schema = new Schema.Parser().parse(new File(args[1]));
        long start = System.currentTimeMillis();
        Params param = new Params(file);
        param.setSchema(schema);
        AvroColumnReader<Record> reader = new AvroColumnReader<Record>(param);

        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        int offsetOrders = schema.getFields().size() - 1;
        Schema orderSchema = schema.getFields().get(schema.getFields().size() - 1).schema().getElementType();
        int offsetLines = orderSchema.getFields().size() - 1;
        Map<String, Map<Long, Float>> values = new HashMap<>();
        while (reader.hasNext()) {
            Record r = reader.next();
            if (!comp) {
                continue;
            }
            String mkts = r.get("c_mktsegment").toString();
            if (mkts.compareTo("BUILDING") != 0) {
                continue;
            }
            List<Record> orders = (List<Record>) r.get(offsetOrders);
            boolean meet = false;
            for (Record order : orders) {
                Long orderkey = (long) order.get("o_orderkey");
                String odate = order.get("o_orderdate").toString();
                if (odate.compareTo("1995-03-15") >= 0) {
                    continue;
                }
                String dateship =
                        order.get("o_orderkey").toString().trim() + "|" + order.get("o_shippriority").toString().trim();
                List<Record> lines = (List<Record>) order.get(offsetLines);
                float value = .0f;
                for (Record line : lines) {
                    String sdate = line.get("l_shipdate").toString();
                    if (sdate.compareTo("1995-03-15") <= 0) {
                        continue;
                    }
                    value += (float) line.get("l_extendedprice") * (1 - (float) line.get("l_discount"));
                    count++;
                    meet = true;
                }
                if (meet) {
                    if (!values.containsKey(dateship)) {
                        values.put(dateship, new HashMap<Long, Float>());
                    }
                    if (!values.get(dateship).containsKey(orderkey)) {
                        values.get(dateship).put(orderkey, .0f);
                    }
                    values.get(dateship).put(orderkey, value);
                }
            }
            //result += (float) r.get(1) * (float) r.get(2);
        }
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(values.size());
        System.out.println(count);
        System.out.println("Trev time: " + (end - start) + " result: " + result);
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }

    public static void Q10_FilteringScan(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema schema = new Schema.Parser().parse(new File(args[1]));
        long start = System.currentTimeMillis();
        Params param = new Params(file);
        param.setSchema(schema);
        AvroColumnReader<Record> reader = new AvroColumnReader<Record>(param);

        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        int offsetOrders = schema.getFields().size() - 1;
        Schema orderSchema = schema.getFields().get(schema.getFields().size() - 1).schema().getElementType();
        int offsetLines = orderSchema.getFields().size() - 1;
        Map<String, Map<Long, Float>> values = new HashMap<>();
        while (reader.hasNext()) {
            Record r = reader.next();
            if (!comp) {
                continue;
            }
            long ck = (long) r.get("c_custkey");
            String ckey = r.get("c_acctbal").toString().trim() + "|" + r.get("c_address").toString().trim() + "|"
                    + (int) r.get("c_nationkey") + "|" + r.get("c_comment").toString().trim();
            List<Record> orders = (List<Record>) r.get(offsetOrders);
            float value = .0f;
            boolean meet = false;
            for (Record order : orders) {
                String odate = order.get("o_orderdate").toString();
                if (odate.compareTo("1993-10-01") < 0 || odate.compareTo("1994-01-01") >= 0) {
                    continue;
                }
                List<Record> lines = (List<Record>) order.get(offsetLines);
                for (Record line : lines) {
                    if (((ByteBuffer) (line.get("l_returnflag"))).compareTo(ByteBuffer.wrap("R".getBytes())) != 0) {
                        continue;
                    }
                    meet = true;
                    value += (float) line.get("l_extendedprice") * (1 - (float) line.get("l_discount"));
                    count++;
                }
            }
            if (meet) {
                if (!values.containsKey(ckey)) {
                    values.put(ckey, new HashMap<Long, Float>());
                }
                if (!values.get(ckey).containsKey(ck)) {
                    values.get(ckey).put(ck, value);
                } else {
                    values.get(ckey).put(ck, values.get(ckey).get(ck) + value);
                }
            }
        }

        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(values.size());
        System.out.println(count);
        System.out.println("Trev time: " + (end - start) + " result: " + result);
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }

    public static void Q15_FilteringScan(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        long start = System.currentTimeMillis();
        Params param = new Params(file);
        param.setSchema(readSchema);
        AvroColumnReader<Record> reader = new AvroColumnReader<Record>(param);

        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        int offsetOrders = readSchema.getFields().size() - 1;
        Schema orderSchema = readSchema.getFields().get(readSchema.getFields().size() - 1).schema().getElementType();
        int offsetLines = orderSchema.getFields().size() - 1;
        while (reader.hasNext()) {
            Record record = reader.next();
            if (!comp) {
                continue;
            }
            List<Record> orders = (List<Record>) record.get(offsetOrders);
            for (Record order : orders) {
                List<Record> lines = (List<Record>) order.get(offsetLines);
                for (Record line : lines) {
                    String shipdate = line.get("l_shipdate").toString();
                    if (shipdate.compareTo("1994-03-02") >= 0 && shipdate.compareTo("1994-06-02") < 0) {
                        result += (float) line.get("l_extendedprice") * (float) line.get("l_discount");
                        count++;
                    }
                }
            }
        }
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("Trev time: " + (end - start) + " result: " + result);
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }

    public static void Q18_FilteringScan(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        long start = System.currentTimeMillis();
        Params param = new Params(file);
        param.setSchema(readSchema);
        AvroColumnReader<Record> reader = new AvroColumnReader<Record>(param);

        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        int offsetOrders = readSchema.getFields().size() - 1;
        Schema orderSchema = readSchema.getFields().get(readSchema.getFields().size() - 1).schema().getElementType();
        int offsetLines = orderSchema.getFields().size() - 1;
        Map<String, Float> results = new HashMap<>();
        while (reader.hasNext()) {
            Record r = reader.next();
            if (!comp) {
                continue;
            }
            String c_name = r.get("c_name").toString();
            long c_ck = (long) r.get("c_custkey");
            List<Record> orders = (List<Record>) r.get(offsetOrders);
            for (Record order : orders) {
                long o_ok = (long) order.get("o_orderkey");
                String o_od = order.get("o_orderdate").toString();
                float o_tp = (float) order.get("o_totalprice");
                List<Record> lines = (List<Record>) order.get(offsetLines);
                float sumqlt = .0f;
                for (Record line : lines) {
                    sumqlt += (float) line.get("l_quantity");
                }
                if (sumqlt > 300) {
                    results.put(c_name + "|" + c_ck + "|" + o_ok + "|" + o_od + "|" + o_tp, sumqlt);
                    count++;
                }
            }
        }

        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(results.size());
        System.out.println(count);
        System.out.println("Trev time: " + (end - start) + " result: " + result);
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        if (args.length != 5) {
            System.out.println("Command: file schema max command blocksize");
            System.exit(-1);
        }
        switch (args[3]) {
            case "q01":
                Q01_FilteringScan(args);
                break;
            case "q03":
                Q03_FilteringScan(args);
                break;
            case "q10":
                Q10_FilteringScan(args);
                break;
            case "q15":
                Q15_FilteringScan(args);
                break;
            case "q18":
                Q18_FilteringScan(args);
                break;
            default:
                break;
        }
    }

}

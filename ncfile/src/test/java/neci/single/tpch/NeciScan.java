/**
 * 
 */
package neci.single.tpch;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import neci.ncfile.BatchColumnReader;
import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.FilterOperator;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;
import neci.parallel.tpch.filter.MktsegmentEqualFilter;
import neci.parallel.tpch.filter.OrderdateSmallerFilter;
import neci.parallel.tpch.filter.ShipdateLargerFilter;
import neci.single.ScanCompare;

/**
 * @author lwh
 *
 */
public class NeciScan extends ScanCompare {
    private static final boolean comp = true;

    public static void Scan(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int blockSize = Integer.parseInt(args[4]);
        long start = System.currentTimeMillis();
        BatchColumnReader<Record> reader = new BatchColumnReader<Record>(file, blockSize);
        reader.createSchema(readSchema);
        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        reader.create();
        int offsetOrders = readSchema.getFields().size() - 1;
        Schema orderSchema = readSchema.getFields().get(readSchema.getFields().size() - 1).schema().getElementType();
        int offsetLines = orderSchema.getFields().size() - 1;
        Map<String, Map<Long, Float>> values = new HashMap<>();
        while (reader.hasNext()) {
            Record r = reader.next();
            if (!comp) {
                continue;
            }
            @SuppressWarnings("unchecked")
            List<Record> orders = (List<Record>) r.get(offsetOrders);
            for (Record order : orders) {
                Long orderkey = (long) order.get("o_orderkey");
                String dateship =
                        order.get("o_orderkey").toString().trim() + "|" + order.get("o_shippriority").toString().trim();
                if (!values.containsKey(dateship)) {
                    values.put(dateship, new HashMap<Long, Float>());
                }
                @SuppressWarnings("unchecked")
                List<Record> lines = (List<Record>) order.get(offsetLines);
                float value = .0f;
                for (Record line : lines) {
                    value += (float) line.get("l_extendedprice") * (1 - (float) line.get("l_discount"));
                    count++;
                }
                if (!values.get(dateship).containsKey(orderkey)) {
                    values.get(dateship).put(orderkey, .0f);
                }
                values.get(dateship).put(orderkey, value);
            }
            //result += (float) r.get(1) * (float) r.get(2);
        }
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(values.size());
        /*for (Byte d : flag) {
            System.out.println(new String(new byte[] { d }));
        }*/
        System.out.println(count);
        System.out.println("NCFile time: " + (end - start) + " result: " + result + " ios: "
                + reader.getBlockManager().getTotalRead() + " aiotime: "
                + reader.getBlockManager().getAioTime() / 1000000 + " iotime: "
                + reader.getBlockManager().getTotalTime() / 1000000 + " compressiontime: "
                + reader.getBlockManager().getCompressionTime() / 1000000 + " created: "
                + reader.getBlockManager().getCreated() + " read: "
                + reader.getBlockManager().getReadLength() / reader.getBlockManager().getTotalRead());
        reader.close();
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }

    public static void FilteringScan(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        int blockSize = Integer.parseInt(args[4]);
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file, blockSize);
        reader.createSchema(readSchema);
        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        reader.createRead(max);
        int offsetOrders = readSchema.getFields().size() - 1;
        Schema orderSchema = readSchema.getFields().get(readSchema.getFields().size() - 1).schema().getElementType();
        int offsetLines = orderSchema.getFields().size() - 1;
        Map<String, Map<Long, Float>> values = new HashMap<>();
        while (reader.hasNext()) {
            Record r = reader.next();
            if (!comp) {
                continue;
            }
            @SuppressWarnings("unchecked")
            List<Record> orders = (List<Record>) r.get(offsetOrders);
            for (Record order : orders) {
                Long orderkey = (long) order.get("o_orderkey");
                String dateship =
                        order.get("o_orderkey").toString().trim() + "|" + order.get("o_shippriority").toString().trim();
                if (!values.containsKey(dateship)) {
                    values.put(dateship, new HashMap<Long, Float>());
                }
                @SuppressWarnings("unchecked")
                List<Record> lines = (List<Record>) order.get(offsetLines);
                float value = .0f;
                for (Record line : lines) {
                    value += (float) line.get("l_extendedprice") * (1 - (float) line.get("l_discount"));
                    count++;
                }
                if (!values.get(dateship).containsKey(orderkey)) {
                    values.get(dateship).put(orderkey, .0f);
                }
                values.get(dateship).put(orderkey, value);
            }
            //result += (float) r.get(1) * (float) r.get(2);
        }
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(values.size());
        /*for (Byte d : flag) {
            System.out.println(new String(new byte[] { d }));
        }*/
        System.out.println(count);
        System.out.println("NCFile time: " + (end - start) + " result: " + result + " ios: "
                + reader.getBlockManager().getTotalRead() + " aiotime: "
                + reader.getBlockManager().getAioTime() / 1000000 + " iotime: "
                + reader.getBlockManager().getTotalTime() / 1000000 + " compressiontime: "
                + reader.getBlockManager().getCompressionTime() / 1000000 + " created: "
                + reader.getBlockManager().getCreated() + " read: "
                + reader.getBlockManager().getReadLength() / reader.getBlockManager().getTotalRead());
        reader.close();
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }

    public static void FilteringNocas(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        int blockSize = Integer.parseInt(args[4]);
        long start = System.currentTimeMillis();
        @SuppressWarnings("rawtypes")
        FilterOperator[] filters = new FilterOperator[3];
        filters[0] = new MktsegmentEqualFilter("BUILDING");
        filters[1] = new OrderdateSmallerFilter("1995-03-15");
        filters[2] = new ShipdateLargerFilter("1995-03-15");
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file, filters, blockSize);
        reader.createSchema(readSchema);
        int count = 0;
        reader.filterNoCasc();
        double result = 0.00;
        double sum = 0.00;
        reader.createFilterRead(max);
        int offsetOrders = readSchema.getFields().size() - 1;
        Schema orderSchema = readSchema.getFields().get(readSchema.getFields().size() - 1).schema().getElementType();
        int offsetLines = orderSchema.getFields().size() - 1;
        Map<String, Map<Long, Float>> values = new HashMap<>();
        while (reader.hasNext()) {
            Record r = reader.next();
            if (!comp) {
                continue;
            }
            @SuppressWarnings("unchecked")
            List<Record> orders = (List<Record>) r.get(offsetOrders);
            for (Record order : orders) {
                Long orderkey = (long) order.get("o_orderkey");
                String dateship =
                        order.get("o_orderkey").toString().trim() + "|" + order.get("o_shippriority").toString().trim();
                if (!values.containsKey(dateship)) {
                    values.put(dateship, new HashMap<Long, Float>());
                }
                @SuppressWarnings("unchecked")
                List<Record> lines = (List<Record>) order.get(offsetLines);
                float value = .0f;
                for (Record line : lines) {
                    value += (float) line.get("l_extendedprice") * (1 - (float) line.get("l_discount"));
                    count++;
                }
                if (!values.get(dateship).containsKey(orderkey)) {
                    values.get(dateship).put(orderkey, .0f);
                }
                values.get(dateship).put(orderkey, value);
            }
            //result += (float) r.get(1) * (float) r.get(2);
        }
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(values.size());
        /*for (Byte d : flag) {
            System.out.println(new String(new byte[] { d }));
        }*/
        System.out.println(count);
        System.out.println("NCFile time: " + (end - start) + " result: " + result + " ios: "
                + reader.getBlockManager().getTotalRead() + " aiotime: "
                + reader.getBlockManager().getAioTime() / 1000000 + " iotime: "
                + reader.getBlockManager().getTotalTime() / 1000000 + " compressiontime: "
                + reader.getBlockManager().getCompressionTime() / 1000000 + " created: "
                + reader.getBlockManager().getCreated() + " read: "
                + reader.getBlockManager().getReadLength() / reader.getBlockManager().getTotalRead());
        reader.close();
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
            case "scan":
                Scan(args);
                break;
            case "filterscan":
                FilteringScan(args);
                break;
            case "nocas":
                FilteringNocas(args);
                break;
        }
    }
}

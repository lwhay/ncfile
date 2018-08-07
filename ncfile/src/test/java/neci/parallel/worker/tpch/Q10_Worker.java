/**
 * 
 */
package neci.parallel.worker.tpch;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;

import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.FilterOperator;
import neci.ncfile.generic.GenericData.Record;
import neci.parallel.tpch.filter.Q10_OrderdateFilter;
import neci.parallel.tpch.filter.Q10_ReturnflagFilter;
import neci.parallel.worker.FilteringScanner;

/**
 * @author Michael
 *
 */
public class Q10_Worker extends FilteringScanner {
    String begin;
    String end;
    ByteBuffer returnflag;

    @Override
    public void config(JsonNode query) throws JsonParseException, IOException {
        JsonNode order = query.path("Order");
        JsonNode o_orderdate = order.path("o_orderdate");
        begin = o_orderdate.path("begin").asText();
        end = o_orderdate.path("end").asText();
        JsonNode lineitem = order.path("LineItem");
        returnflag = ByteBuffer.wrap(lineitem.path("l_returnflag").asText().getBytes());
    }

    @Override
    public void run() {
        File file = new File(path);
        //The schema is only used for fetching operation as filters have been contained by query.
        FilterOperator[] filters = new FilterOperator[2];
        filters[0] = new Q10_OrderdateFilter(begin, end);
        filters[1] = new Q10_ReturnflagFilter(returnflag);
        long start = System.currentTimeMillis();
        try {
            reader = new FilterBatchColumnReader<Record>(file, filters, blockSize);
            reader.createSchema(schema);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        long t1 = System.currentTimeMillis();
        try {
            reader.filter();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        long t2 = System.currentTimeMillis();
        try {
            reader.createFilterRead(batchSize);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        double result = 0.00;
        int count = 0;
        Map<String, Map<Long, Float>> values = new HashMap<>();
        while (reader.hasNext()) {
            Record r = reader.next();
            long ck = (long) r.get("c_custkey");
            String ckey = r.get("c_acctbal").toString().trim() + "|" + r.get("c_address").toString().trim() + "|"
                    + r.get("c_nationkey").toString().trim() + "|" + r.get("c_comment").toString().trim();
            if (!values.containsKey(ckey)) {
                values.put(ckey, new HashMap<Long, Float>());
            }
            List<Record> orders = (List<Record>) r.get(schema.getFields().size() - 1);
            float value = .0f;
            for (Record order : orders) {
                List<Record> lines = (List<Record>) order.get("LineitemList");
                for (Record line : lines) {
                    value += (float) line.get("l_extendedprice") * (1 - (float) line.get("l_discount"));
                    count++;
                }
            }
            if (!values.get(ckey).containsKey(ck)) {
                values.get(ckey).put(ck, value);
            } else {
                values.get(ckey).put(ck, values.get(ckey).get(ck) + value);
            }
        }
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("total: " + (end - start) + "ms filter: " + (t2 - t1) + "ms result: " + result + " count: "
                + count + " reads: " + reader.getBlockManager().getTotalRead() + " iotime: "
                + reader.getBlockManager().getTotalTime() / 1000000 + " created: "
                + reader.getBlockManager().getCreated() + " read: "
                + reader.getBlockManager().getReadLength() / reader.getBlockManager().getTotalRead());
    }
}

/**
 * 
 */
package neci.parallel.worker.tpch;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;

import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.FilterOperator;
import neci.ncfile.generic.GenericData.Record;
import neci.parallel.tpch.filter.Q03_MktsegmentFilter;
import neci.parallel.tpch.filter.Q03_OrderdateFilter;
import neci.parallel.tpch.filter.Q03_ShipdateFilter;
import neci.parallel.worker.FilteringScanner;

/**
 * @author Michael
 *
 */
public class Q03_Worker extends FilteringScanner {
    String mktsegment;
    String orderdate;
    String shipdate;

    @Override
    public void config(JsonNode query) throws JsonParseException, IOException {
        mktsegment = query.path("c_mktsegment").asText();
        JsonNode order = query.path("Order");
        JsonNode o_orderdate = order.path("o_orderdate");
        orderdate = o_orderdate.path("end").asText();
        JsonNode lineitem = order.path("LineItem");
        shipdate = lineitem.path("begin").asText();
    }

    @Override
    public void run() {
        File file = new File(path);
        //The schema is only used for fetching operation as filters have been contained by query.
        FilterOperator[] filters = new FilterOperator[3];
        filters[0] = new Q03_MktsegmentFilter(mktsegment);
        filters[1] = new Q03_OrderdateFilter(orderdate);
        filters[2] = new Q03_ShipdateFilter(shipdate); //l_shipdate
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
            List<Record> orders = (List<Record>) r.get(schema.getFields().size() - 1);
            for (Record order : orders) {
                Long orderkey = (long) order.get("o_orderkey");
                String dateship =
                        order.get("o_orderkey").toString().trim() + "|" + order.get("o_shippriority").toString().trim();
                if (!values.containsKey(dateship)) {
                    values.put(dateship, new HashMap<Long, Float>());
                }
                List<Record> lines = (List<Record>) order.get("LineitemList");
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
        try {
            reader.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
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

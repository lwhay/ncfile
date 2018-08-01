/**
 * 
 */
package neci.worker.tpch;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;

import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.FilterOperator;
import neci.ncfile.generic.GenericData.Record;
import neci.parallel.worker.FilteringScanner;
import tpch.single.Q15_ShipdateFilter;

/**
 * @author Michael
 *
 */
public class Q15_Worker extends FilteringScanner {
    String begin;
    String end;

    public void config(JsonNode query) throws JsonParseException, IOException {
        /*JsonParser fact = new JsonFactory().createJsonParser(query);
        JsonNode root = fact.readValueAsTree();*/
        JsonNode lienitem = query.path("LineItem");
        JsonNode shipdate = lienitem.path("l_shipdate");

        begin = shipdate.path("begin").asText();
        end = shipdate.path("end").asText();
        //if (shipdate.isArray()) {
        /*Iterator<Entry<String, JsonNode>> iter = root.getFields();
        int i = 0;
        do {
        shipdates[i] = iter.next().getValue().getTextValue();
        } while (iter.hasNext());*/
        //}
    }

    @Override
    public void run() {
        File file = new File(path);
        //The schema is only used for fetching operation as filters have been contained by query.
        FilterOperator[] filters = new FilterOperator[1];
        filters[0] = new Q15_ShipdateFilter(begin, end); //l_shipdate
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
        while (reader.hasNext()) {
            Record r = reader.next();
            //result += (float) r.get(1) * (float) r.get(2);
            count++;
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

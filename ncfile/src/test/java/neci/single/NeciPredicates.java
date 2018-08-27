/**
 * 
 */
package neci.single;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.FilterOperator;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;
import neci.parallel.tpch.filter.CnationkeyContainedbyFilter;
import neci.parallel.tpch.filter.LsuppkeyContainedbyFilter;
import neci.parallel.tpch.filter.ShipdateBetweenFilter;
import utils.tpch.SupplierHelper;

/**
 * @author Michael
 *
 */
public class NeciPredicates {
    private static final boolean comp = new File("./comp.mark").exists();

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        int nationCount = Integer.parseInt(args[3]);
        int blockSize = Integer.parseInt(args[4]);
        long start = System.currentTimeMillis();
        Set<String> nations = new HashSet<>();
        for (int i = 0; i < nationCount; i++) {
            nations.add(args[5 + i]);
        }
        BitSet indicators = new BitSet();
        SupplierHelper minSuppcost = new SupplierHelper("ANY", indicators);
        minSuppcost.init();
        Map<Long, String[]> supplyNations = minSuppcost.getValidSuppCosts();
        Map<Integer, String> nationNames = minSuppcost.getNationNames();
        @SuppressWarnings("rawtypes")
        FilterOperator[] filters = new FilterOperator[3];
        // We have applied innermost verification for the equality of equal-region supplier and customer.
        filters[0] = new CnationkeyContainedbyFilter(nationNames.keySet());
        filters[1] = new LsuppkeyContainedbyFilter(supplyNations.keySet());
        filters[2] = new ShipdateBetweenFilter("1994-01-01", "1995-01-01");
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file, filters, blockSize);
        reader.createSchema(readSchema);
        int count = 0;
        reader.filter();
        double result = 0.00;
        double sum = 0.00;
        reader.createFilterRead(max);
        int offsetOrders = readSchema.getFields().size() - 1;
        Schema orderSchema = readSchema.getFields().get(readSchema.getFields().size() - 1).schema().getElementType();
        int offsetLines = orderSchema.getFields().size() - 1;
        Map<String, Float> values = new HashMap<>();
        int redundant = 0;
        while (reader.hasNext()) {
            Record r = reader.next();
            if (!comp) {
                continue;
            }
            int c_nationkey = (int) r.get("c_nationkey");
            @SuppressWarnings("unchecked")
            List<Record> orders = (List<Record>) r.get(offsetOrders);
            for (Record order : orders) {
                @SuppressWarnings("unchecked")
                List<Record> lines = (List<Record>) order.get(offsetLines);
                for (Record line : lines) {
                    long l_suppkey = (long) line.get("l_suppkey");
                    int suppNationKey = Integer.parseInt(supplyNations.get(l_suppkey)[0]);
                    if (suppNationKey != c_nationkey) {
                        String key = "";
                        key += nationNames.get(c_nationkey);
                        key += "|";
                        key += nationNames.get(suppNationKey);
                        key += "|";
                        key += line.get("l_shipdate").toString().substring(0, 4);
                        float value = .0f;
                        if (values.containsKey(key)) {
                            value = values.get(key);
                        }
                        value += (float) line.get("l_extendedprice") * (1 - (float) line.get("l_discount"));
                        values.put(key, value);
                    } else {
                        redundant++;
                    }
                    count++;
                }
            }
        }
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(values.size() + " fine-grained: " + count + " redundant: " + redundant);
        System.out.println("NCFile time: " + (end - start) + " result: " + result + " ios: "
                + reader.getBlockManager().getTotalRead() + " aiotime: "
                + reader.getBlockManager().getAioTime() / 1000000 + " aioFetchtime: "
                + reader.getBlockManager().getAioFetchTime() / 1000000 + " iotime: "
                + reader.getBlockManager().getTotalTime() / 1000000 + " compressiontime: "
                + reader.getBlockManager().getCompressionTime() / 1000000 + " created: "
                + reader.getBlockManager().getCreated() + " read: "
                + reader.getBlockManager().getReadLength() / reader.getBlockManager().getTotalRead());
        reader.close();
    }

}

/**
 * 
 */
package tpch.single;

import java.io.File;
import java.io.IOException;

import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.FilterOperator;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

/**
 * @author Michael
 *
 */
public class Q15 {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        FilterOperator[] filters = new FilterOperator[1];
        filters[0] = new Q15_ShipdateFilter(args[3], args[4]); //l_shipdate
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file, filters);
        reader.createSchema(readSchema);
        long t1 = System.currentTimeMillis();
        reader.filter();
        long t2 = System.currentTimeMillis();
        reader.createFilterRead(max);
        int count = 0;
        int sumC = reader.getRowCount(reader.getValidColumnNO(readSchema.getFields().get(0).name()));
        double result = 0.00;
        while (reader.hasNext()) {
            Record r = reader.next();
            result += (float) r.get(0) * (float) r.get(1);
            count++;
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println(sumC);
        System.out.println("time: " + (end - start));
        System.out.println("filter time: " + (t2 - t1));
    }

}

/**
 * 
 */
package tpch.single;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.FilterOperator;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

/**
 * @author Michael
 *
 */
public class Q01 {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        FilterOperator[] filters = new FilterOperator[1];
        filters[0] = new Q01_ShipdateFilter(args[3]);
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file, filters);
        reader.createSchema(readSchema);
        long t1 = System.currentTimeMillis();
        reader.filterNoCasc();
        long t2 = System.currentTimeMillis();
        reader.createFilterRead(max);
        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        //byte[] key = new byte[2];
        byte[] tmp = new byte[2];
        long selectivity = 0;
        Map<String, float[]> gbyMap = new HashMap<>();
        while (reader.hasNext()) {
            Record r = reader.next();
            float quantity = (float) r.get(0);
            float extendedprice = (float) r.get(1);
            float discount = (float) r.get(2);
            float tax = (float) r.get(3);
            byte[] returnflag = ((ByteBuffer) r.get(4)).array();
            byte[] linestatus = ((ByteBuffer) r.get(5)).array();
            if (returnflag.length != 1 || linestatus.length != 1) {
                System.out.println("need to handle this." + " rf: " + returnflag[0] + " out of: " + returnflag.length
                        + " ls: " + linestatus[0] + " out of: " + linestatus.length + " tk: " + selectivity);
            } else {
                //System.out.println(selectivity + " out of: " + reader.getRowCount(0));
            }
            tmp[0] = returnflag[0];
            tmp[1] = linestatus[0];
            String bb = new String(tmp);
            if (!gbyMap.containsKey(bb)) {
                byte[] key = new byte[2];
                key[0] = tmp[0];
                key[1] = tmp[1];
                gbyMap.put(new String(key), new float[8]);
            }
            float[] values = gbyMap.get(bb);
            values[0] += quantity;
            values[1] += extendedprice;
            float fact = extendedprice * (1 - discount);
            values[2] += fact;
            values[3] += fact * (1 + tax);
            values[4] += quantity;
            values[5] += extendedprice;
            values[6] += discount;
            values[7]++;
            selectivity++;
        }
        //        reader.filterReadIO();
        //        long timeIO = reader.getTimeIO();
        //        int[] filterBlock = reader.getFilterBlock();
        //        int[] seekBlockRes = reader.getBlockSeekRes();
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        System.out.println("filter time: " + (t2 - t1));
        //        System.out.println("IO time: " + timeIO);
        //        System.out.println("***********************filterBlockRes************");
        //        System.out.println("read Block: " + filterBlock[0] + "\tseeked Block: " + filterBlock[1] + "\tblock count: "
        //                + filterBlock[2]);
        //        System.out.println("***********************allBlockRes************");
        //        System.out.println("read Block: " + seekBlockRes[0] + "\tseeked Block: " + seekBlockRes[1] + "\tblock count: "
        //                + seekBlockRes[2]);
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("selectivity: " + selectivity);
        System.out.println("Result:");
        for (Map.Entry<String, float[]> entry : gbyMap.entrySet()) {
            System.out.println(entry.getKey().charAt(0) + "," + entry.getKey().charAt(1) + ":" + entry.getValue()[0]
                    + "," + entry.getValue()[1] + "," + entry.getValue()[2] + "," + entry.getValue()[3] + ","
                    + entry.getValue()[4] / entry.getValue()[7] + "," + entry.getValue()[5] / entry.getValue()[7] + ","
                    + entry.getValue()[6] / entry.getValue()[7] + "," + (long) entry.getValue()[7]);
        }
    }

}

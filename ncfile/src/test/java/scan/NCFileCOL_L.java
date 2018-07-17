/**
 * 
 */
package scan;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.generic.GenericData.Record;

import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.base.Schema;

/**
 * @author lwh
 *
 */
public class NCFileCOL_L {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Command: file schema max");
            System.exit(-1);
        }
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(file);
        reader.createSchema(readSchema);
        reader.createRead(max);
        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        System.out.println("line: " + reader.getRowCount(0));
        while (reader.hasNext()) {
            Record r = reader.next();
            List<Record> orders = (List<Record>) r.get(8);
            for (Record order : orders) {
                List<Record> lines = (List<Record>) order.get(9);
                count += lines.size();
                for (Record line : lines) {
                    double res = (float) line.get(5) * (1 - (float) line.get(6));
                    sum += res;
                    result += res;
                }
            }
            System.out.println(result);
        }
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("time: " + (end - start));
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }
}

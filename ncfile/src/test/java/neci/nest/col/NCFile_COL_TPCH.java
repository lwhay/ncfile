/**
 * 
 */
package neci.nest.col;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.FilterOperator;
import neci.parallel.tpch.filter.Q03_MktsegmentFilter;
import neci.parallel.tpch.filter.Q03_OrderdateFilter;
import neci.parallel.tpch.filter.Q03_ShipdateFilter;
import neci.parallel.tpch.filter.Q10_OrderdateFilter;
import neci.parallel.tpch.filter.Q10_ReturnflagFilter;
import neci.parallel.tpch.filter.Q15_ShipdateFilter;

/**
 * @author lwh
 *
 */
public class NCFile_COL_TPCH extends NCFile_COL_Scan {

    public static void Q03_FilteringScan(String[] args) throws IOException {
        File file = new File(args[0]);
        neci.ncfile.base.Schema readSchema = new neci.ncfile.base.Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        int blockSize = Integer.parseInt(args[4]);
        long start = System.currentTimeMillis();
        FilterOperator[] filters = new FilterOperator[3];
        filters[0] = new Q03_MktsegmentFilter("BUILDING");
        filters[1] = new Q03_OrderdateFilter("1995-03-15");
        filters[2] = new Q03_ShipdateFilter("1995-03-15");
        FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record> reader =
                new FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record>(file, filters, blockSize);
        reader.createSchema(readSchema);
        int count = 0;
        reader.filter();
        double result = 0.00;
        double sum = 0.00;
        reader.createFilterRead(max);
        //reader.createRead(max);
        //Set<String> date = new HashSet<String>();
        //Set<Byte> flag = new HashSet<>();
        String name = readSchema.getFields().get(0).name();
        int columnNo = reader.getValidColumnNO(name);
        System.out.println("line: " + reader.getRowCount(columnNo));
        while (reader.hasNext()) {
            neci.ncfile.generic.GenericData.Record r = reader.next();
            //System.out.println(r);
            List<neci.ncfile.generic.GenericData.Record> orders =
                    (List<neci.ncfile.generic.GenericData.Record>) r.get(0);
            for (neci.ncfile.generic.GenericData.Record order : orders) {
                //date.add(((String) order.get(0)).substring(0, 4));
                List<neci.ncfile.generic.GenericData.Record> lines =
                        (List<neci.ncfile.generic.GenericData.Record>) order.get(2);
                count += lines.size();
                for (neci.ncfile.generic.GenericData.Record line : lines) {
                    double res = (float) line.get(1) * (1 - (float) line.get(2));
                    //flag.add(((ByteBuffer) line.get(2)).get(0));
                    sum += res;
                    result += res;
                }
            }
            //System.out.println(result);
        }
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        /*for (Byte d : flag) {
            System.out.println(new String(new byte[] { d }));
        }*/
        System.out.println(count);
        System.out.println("NCFile time: " + (end - start) + " result: " + result + " ios: "
                + reader.getBlockManager().getTotalRead() + " iotime: "
                + reader.getBlockManager().getTotalTime() / 1000000 + " created: "
                + reader.getBlockManager().getCreated() + " read: "
                + reader.getBlockManager().getReadLength() / reader.getBlockManager().getTotalRead());
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }

    public static void Q10_FilteringScan(String[] args) throws IOException {
        File file = new File(args[0]);
        neci.ncfile.base.Schema readSchema = new neci.ncfile.base.Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        int blockSize = Integer.parseInt(args[4]);
        long start = System.currentTimeMillis();
        FilterOperator[] filters = new FilterOperator[2];
        filters[0] = new Q10_OrderdateFilter("1993-10-01", "1994-01-01");
        filters[1] = new Q10_ReturnflagFilter(ByteBuffer.wrap("R".getBytes()));
        FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record> reader =
                new FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record>(file, filters, blockSize);
        reader.createSchema(readSchema);
        //reader.createRead(max);
        int count = 0;
        reader.filter();
        double result = 0.00;
        double sum = 0.00;
        reader.createFilterRead(max);
        //Set<String> date = new HashSet<String>();
        //Set<Byte> flag = new HashSet<>();
        System.out
                .println("line: " + reader.getRowCount(reader.getValidColumnNO(readSchema.getFields().get(0).name())));
        while (reader.hasNext()) {
            neci.ncfile.generic.GenericData.Record r = reader.next();
            //System.out.println(r);
            List<neci.ncfile.generic.GenericData.Record> orders =
                    (List<neci.ncfile.generic.GenericData.Record>) r.get(6);
            for (neci.ncfile.generic.GenericData.Record order : orders) {
                //date.add(((String) order.get(0)).substring(0, 4));
                List<neci.ncfile.generic.GenericData.Record> lines =
                        (List<neci.ncfile.generic.GenericData.Record>) order.get(0);
                count += lines.size();
                for (neci.ncfile.generic.GenericData.Record line : lines) {
                    double res = (float) line.get(0) * (1 - (float) line.get(1));
                    //flag.add(((ByteBuffer) line.get(2)).get(0));
                    sum += res;
                    result += res;
                }
            }
            //System.out.println(result);
        }
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        /*for (Byte d : flag) {
            System.out.println(new String(new byte[] { d }));
        }*/
        System.out.println(count);
        System.out.println("NCFile time: " + (end - start) + " result: " + result + " ios: "
                + reader.getBlockManager().getTotalRead() + " iotime: "
                + reader.getBlockManager().getTotalTime() / 1000000 + " created: "
                + reader.getBlockManager().getCreated() + " read: "
                + reader.getBlockManager().getReadLength() / reader.getBlockManager().getTotalRead());
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }

    public static void Q15_FilteringScan(String[] args) throws IOException {
        File file = new File(args[0]);
        neci.ncfile.base.Schema readSchema = new neci.ncfile.base.Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        int blockSize = Integer.parseInt(args[4]);
        long start = System.currentTimeMillis();
        FilterOperator[] filters = new FilterOperator[1];
        filters[0] = new Q15_ShipdateFilter("1994-09-01", "1994-12-01");
        FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record> reader =
                new FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record>(file, filters, blockSize);
        reader.createSchema(readSchema);
        //reader.createRead(max);
        int count = 0;
        reader.filter();
        double result = 0.00;
        double sum = 0.00;
        reader.createFilterRead(max);
        //Set<String> date = new HashSet<String>();
        //Set<Byte> flag = new HashSet<>();
        System.out
                .println("line: " + reader.getRowCount(reader.getValidColumnNO(readSchema.getFields().get(0).name())));
        while (reader.hasNext()) {
            neci.ncfile.generic.GenericData.Record r = reader.next();
            //System.out.println(r);
            result += (float) r.get(1) * (float) r.get(2);
            count++;
            //System.out.println(result);
        }
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        /*for (Byte d : flag) {
            System.out.println(new String(new byte[] { d }));
        }*/
        System.out.println(count);
        System.out.println("NCFile time: " + (end - start) + " result: " + result + " ios: "
                + reader.getBlockManager().getTotalRead() + " iotime: "
                + reader.getBlockManager().getTotalTime() / 1000000 + " created: "
                + reader.getBlockManager().getCreated() + " read: "
                + reader.getBlockManager().getReadLength() / reader.getBlockManager().getTotalRead());
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
            System.out.println("Command: file schema max");
            System.exit(-1);
        }
        switch (args[3]) {
            case "q03":
                Q03_FilteringScan(args);
                break;
            case "q10":
                Q10_FilteringScan(args);
                break;
            case "q15":
                Q15_FilteringScan(args);
                break;
            case "avro":
                scanAvro(args);
                break;
            case "trev":
                scanTrev(args);
                break;
            case "parq":
                break;
            default:
                break;
        }
    }
}

/**
 * 
 */
package scan;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;

import neci.ncfile.FilterBatchColumnReader;

/**
 * @author lwh
 *
 */
public class NCFileCOL_L {

    public static void scanNCFile(String[] args) throws IOException {
        File file = new File(args[0]);
        neci.ncfile.base.Schema readSchema = new neci.ncfile.base.Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record> reader =
                new FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record>(file, 32);
        reader.createSchema(readSchema);
        reader.createRead(max);
        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        System.out.println("line: " + reader.getRowCount(0));
        while (reader.hasNext()) {
            neci.ncfile.generic.GenericData.Record r = reader.next();
            //System.out.println(r);
            /*List<neci.ncfile.generic.GenericData.Record> orders =
                    (List<neci.ncfile.generic.GenericData.Record>) r.get(8);
            for (neci.ncfile.generic.GenericData.Record order : orders) {
                List<neci.ncfile.generic.GenericData.Record> lines =
                        (List<neci.ncfile.generic.GenericData.Record>) order.get(9);
                count += lines.size();
                for (neci.ncfile.generic.GenericData.Record line : lines) {
                    double res = (float) line.get(5) * (1 - (float) line.get(6));
                    sum += res;
                    result += res;
                }
            }*/
            //System.out.println(result);
        }
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("NCFile time: " + (end - start) + " result: " + result);
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }

    public static void scanAvro(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        long start = System.currentTimeMillis();
        DatumReader<Record> gr = new GenericDatumReader<Record>(readSchema);
        DataFileReader<Record> reader = new DataFileReader<Record>(file, gr);

        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        while (reader.hasNext()) {
            Record r = reader.next();
            //System.out.println(r);
            /*List<Record> orders = (List<Record>) r.get(8);
            for (Record order : orders) {
                List<Record> lines = (List<Record>) order.get(9);
                count += lines.size();
                for (Record line : lines) {
                    double res = (float) line.get(5) * (1 - (float) line.get(6));
                    sum += res;
                    result += res;
                }
            }*/
            //System.out.println(result);
        }
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("Avro time: " + (end - start) + " result: " + result);
        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        System.out.println("revenue: " + nf.format(result));
    }

    public static void scanTrev(String[] args) throws IOException {
        File file = new File(args[0]);
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        long start = System.currentTimeMillis();
        Params param = new Params(file);
        param.setSchema(readSchema);
        AvroColumnReader<Record> reader = new AvroColumnReader<Record>(param);

        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        while (reader.hasNext()) {
            Record r = reader.next();
            //System.out.println(r);
            /*List<Record> orders = (List<Record>) r.get(8);
            for (Record order : orders) {
                List<Record> lines = (List<Record>) order.get(9);
                count += lines.size();
                for (Record line : lines) {
                    double res = (float) line.get(5) * (1 - (float) line.get(6));
                    sum += res;
                    result += res;
                }
            }*/
            //System.out.println(result);
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

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        if (args.length != 4) {
            System.out.println("Command: file schema max");
            System.exit(-1);
        }
        switch (args[3]) {
            case "neci":
                scanNCFile(args);
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

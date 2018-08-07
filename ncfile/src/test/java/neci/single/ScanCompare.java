/**
 * 
 */
package neci.single;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;

import neci.ncfile.FilterBatchColumnReader;

/**
 * @author lwh
 *
 */
public class ScanCompare {
    private static final boolean comp = new File("comp.mark").exists();

    public static void scanNCFile(String[] args) throws IOException {
        File file = new File(args[0]);
        neci.ncfile.base.Schema readSchema = new neci.ncfile.base.Schema.Parser().parse(new File(args[1]));
        int max = Integer.parseInt(args[2]);
        int blockSize = Integer.parseInt(args[4]);
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record> reader =
                new FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record>(file, blockSize);
        reader.createSchema(readSchema);
        reader.createRead(max);
        int count = 0;
        double result = 0.00;
        double sum = 0.00;
        System.out
                .println("line: " + reader.getRowCount(reader.getValidColumnNO(readSchema.getFields().get(0).name())));
        while (reader.hasNext()) {
            neci.ncfile.generic.GenericData.Record r = reader.next();
            //System.out.println(r);
            if (comp) {
                List<neci.ncfile.generic.GenericData.Record> orders =
                        (List<neci.ncfile.generic.GenericData.Record>) r.get(readSchema.getFields().size() - 1);
                for (neci.ncfile.generic.GenericData.Record order : orders) {
                    neci.ncfile.base.Schema orderSchema =
                            readSchema.getFields().get(readSchema.getFields().size() - 1).schema().getElementType();
                    List<neci.ncfile.generic.GenericData.Record> lines =
                            (List<neci.ncfile.generic.GenericData.Record>) order
                                    .get(orderSchema.getFields().size() - 1);
                    //count += lines.size();
                    for (neci.ncfile.generic.GenericData.Record line : lines) {
                        double res = (float) line.get(5) * (1 - (float) line.get(6));
                        sum += res;
                        result += res;
                        count++;
                    }
                }
            }
            //System.out.println(result);
        }
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("NCFile time: " + (end - start) + " result: " + result + " ios: "
                + reader.getBlockManager().getTotalRead() + " iotime: "
                + reader.getBlockManager().getTotalTime() / 1000000 + " compressiontime: "
                + reader.getBlockManager().getCompressionTime() / 1000000 + " created: "
                + reader.getBlockManager().getCreated() + " read: "
                + reader.getBlockManager().getReadLength() / reader.getBlockManager().getTotalRead());
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
        int offsetOrders = readSchema.getFields().size() - 1;
        Schema orderSchema = readSchema.getFields().get(readSchema.getFields().size() - 1).schema().getElementType();
        int offsetLines = orderSchema.getFields().size() - 1;
        while (reader.hasNext()) {
            Record record = reader.next();
            if (comp) {
                List<Record> orders = (List<Record>) record.get(offsetOrders);
                for (Record order : orders) {
                    List<Record> lines = (List<Record>) order.get(offsetLines);
                    for (Record line : lines) {
                        double res = (float) line.get(5) * (1 - (float) line.get(6));
                        sum += res;
                        result += res;
                        count++;
                    }
                }
            }
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
        int offsetOrders = readSchema.getFields().size() - 1;
        Schema orderSchema = readSchema.getFields().get(readSchema.getFields().size() - 1).schema().getElementType();
        int offsetLines = orderSchema.getFields().size() - 1;
        while (reader.hasNext()) {
            Record record = reader.next();
            if (comp) {
                List<Record> orders = (List<Record>) record.get(offsetOrders);
                for (Record order : orders) {
                    List<Record> lines = (List<Record>) order.get(offsetLines);
                    for (Record line : lines) {
                        double res = (float) line.get(5) * (1 - (float) line.get(6));
                        sum += res;
                        result += res;
                        count++;
                    }
                }
            }
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

    public static void scanParq(String[] args) throws IOException {
        String path = args[0];
        Schema readSchema = new Schema.Parser().parse(new File(args[1]));
        String outline = "";
        System.out.println(path);
        outline = "\tBegin path: " + path + " ";
        long begin = System.currentTimeMillis();
        ParquetReader<GenericRecord> reader;
        Configuration conf = new Configuration();
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<GenericRecord>();
        readSupport.setRequestedProjection(conf, readSchema);
        readSupport.setAvroReadSchema(conf, readSchema);
        reader = new ParquetReader(conf, new Path(path), readSupport);
        outline += " prepared: " + (System.currentTimeMillis() - begin) + " ";
        begin = System.currentTimeMillis();
        int count = 1;
        double result = 0.00;
        double sum = 0.00;
        int offsetOrders = readSchema.getFields().size() - 1;
        Schema orderSchema = readSchema.getFields().get(readSchema.getFields().size() - 1).schema().getElementType();
        int offsetLines = orderSchema.getFields().size() - 1;
        GenericRecord record;
        while ((record = reader.read()) != null) {
            if (comp) {
                List<Record> orders = (List<Record>) record.get(offsetOrders);
                for (Record order : orders) {
                    List<Record> lines = (List<Record>) order.get(offsetLines);
                    for (Record line : lines) {
                        double res = (float) line.get(5) * (1 - (float) line.get(6));
                        sum += res;
                        result += res;
                        count++;
                    }
                }
            }
        }
        /*if (count % 1000000 == 0) {
            System.out.println(count);
        }*/
        reader.close();
        result = result / sum * 100;
        long end = System.currentTimeMillis();
        System.out.println(count);
        System.out.println("Parq time: " + (end - begin) + " result: " + result);
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
                scanParq(args);
                break;
            default:
                break;
        }
    }
}

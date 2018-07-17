package misc;
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
//import java.io.IOException;
//
//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericData.Record;
//import org.apache.trevni.avro.update.BloomCount;
//import org.apache.trevni.avro.update.BloomFilter;
//import org.apache.trevni.avro.update.BloomFilter.BloomFilterBuilder;
//import org.apache.trevni.avro.update.BloomFilterModel;
//
public class BloomTest {
    //    static Schema lkschema = new Schema.Parser().parse(
    //            "{\"name\": \"Lineitem\", \"type\": \"record\", \"fields\":[{\"name\": \"l_orderkey\", \"type\": \"long\"}, {\"name\": \"l_linenumber\", \"type\": \"int\"}]}");
    //
    //    public static void lkTest(String path, String bloompath, int numElements) throws IOException {
    //        int numBucketsPerElement = BloomCount.maxBucketPerElement(numElements);
    //        BloomFilterModel model = BloomCount.computeBloomModel(numBucketsPerElement, 0.01);
    //        File bloomfile = new File(bloompath);
    //        if (!bloomfile.exists()) {
    //            bloomfile.mkdir();
    //        }
    //        BloomFilter filter = new BloomFilter(bloomfile, lkschema);
    //        BloomFilterBuilder builder = filter.creatBuilder(numElements, model.getNumHashes(),
    //                model.getNumBucketsPerElement());
    //        System.out.println("numHashes:" + model.getNumHashes() + "\tnumBitsPerElement:"
    //                + model.getNumBucketsPerElement() + "\tnumElements:" + numElements + "\tnumBytes:"
    //                + (numElements * model.getNumBucketsPerElement() / 8));
    //        File file = new File(path);
    //        BufferedReader reader = new BufferedReader(new FileReader(file));
    //        Record record = new Record(lkschema);
    //        String line;
    //        while ((line = reader.readLine()) != null) {
    //            String[] tmp = line.split("\\|", 5);
    //            record.put(0, Long.parseLong(tmp[0]));
    //            record.put(1, Integer.parseInt(tmp[3]));
    //            builder.add(record);
    //        }
    //        reader.close();
    //        builder.write();
    //        filter.activate();
    //        long t = 0;
    //        long f = 0;
    //        BufferedReader checkReader = new BufferedReader(new FileReader(file));
    //        long[] hashes = new long[2];
    //        while ((line = checkReader.readLine()) != null) {
    //            String[] tmp = line.split("\\|", 5);
    //            record.put(0, Long.parseLong(tmp[0]));
    //            record.put(1, Integer.parseInt(tmp[3]));
    //            boolean x = filter.contains(record, hashes);
    //            if (x) {
    //                t++;
    //            } else {
    //                f++;
    //            }
    //        }
    //        checkReader.close();
    //        System.out.println("true: " + t + "\tfalse: " + f);
    //    }
    //
    //    public static void falsePositiveTest(String path, String bloompath) throws IOException {
    //        BloomFilter filter = new BloomFilter(new File(bloompath), lkschema);
    //        filter.activate();
    //        String line;
    //        Record record = new Record(lkschema);
    //        BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
    //        long[] hashes = new long[2];
    //        long t = 0, f = 0;
    //        while ((line = reader.readLine()) != null) {
    //            String[] tmp = line.split("\\|", 5);
    //            record.put(0, Long.parseLong(tmp[0]));
    //            record.put(1, Integer.parseInt(tmp[3]));
    //            boolean x = filter.contains(record, hashes);
    //            if (x) {
    //                t++;
    //            } else {
    //                f++;
    //            }
    //        }
    //        reader.close();
    //        System.out.println("true: " + t + "\tfalse: " + f + "\tfalse-negative(%): " + ((double) t / (t + f)));
    //    }
    //
    //    public static void main(String[] args) throws IOException {
    //        if (args[0].equals("true")) {
    //            lkTest(args[1], args[2], Integer.parseInt(args[3]));
    //        }
    //        if (args[0].equals("false")) {
    //            falsePositiveTest(args[1], args[2]);
    //        }
    //        if (args[0].equals("lk")) {
    //            lkTest(args[1], args[3], Integer.parseInt(args[4]));
    //            falsePositiveTest(args[2], args[3]);
    //        }
    //    }
}

/**
 * 
 */
package neci.translation;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnWriter;

import neci.ncfile.FilterBatchColumnReader;

/**
 * @author iclab
 *
 */
public class NCFileTranToTrevCodec {
    public static void main(String[] args) throws IOException {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        File fromFile = new File(args[1]);
        File toFile1 = new File(args[2]);
        int max = Integer.parseInt(args[4]);
        ColumnFileMetaData metaData = new ColumnFileMetaData().setCodec(args[5]);
        AvroColumnWriter<Record> writer1 = new AvroColumnWriter<Record>(s, metaData);
        if (!toFile1.getParentFile().exists()) {
            toFile1.getParentFile().mkdirs();
        }

        FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record> reader =
                new FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record>(fromFile);
        reader.createSchema(new neci.ncfile.base.Schema.Parser().parse(new File(args[0])));
        //        long t1 = System.currentTimeMillis();
        //        reader.filterNoCasc();
        //        long t2 = System.currentTimeMillis();
        reader.createRead(max);

        while (reader.hasNext()) {
            neci.ncfile.generic.GenericData.Record record = reader.next();
            Record r = translate(record, s);
            //System.out.println(r);
            //writer1.write(r);
            writer1.write(r);
        }
        reader.close();
        writer1.writeTo(toFile1);
    }

    public static Record translate(neci.ncfile.generic.GenericData.Record record, Schema s) {
        Record r = new Record(s);
        for (int i = 0; i < s.getFields().size(); i++) {
            switch (s.getFields().get(i).schema().getType()) {
                case STRING:
                    r.put(i, record.get(i).toString());
                    break;
                case INT:
                    r.put(i, (int) record.get(i));
                    break;
                case LONG:
                    r.put(i, (long) record.get(i));
                    break;
                case FLOAT:
                    r.put(i, (float) record.get(i));
                    break;
                case DOUBLE:
                    r.put(i, (double) record.get(i));
                    break;
                case BYTES:
                    r.put(i, (ByteBuffer) (record.get(i)));
                    break;
                case BOOLEAN:
                    r.put(i, (boolean) record.get(i));
                    break;
                case RECORD: {
                    Schema ns = s.getFields().get(i).schema();
                    r.put(i, translate((neci.ncfile.generic.GenericData.Record) record.get(i), ns));
                    break;
                }
                case ARRAY: {
                    List<neci.ncfile.generic.GenericData.Record> nrs =
                            (List<neci.ncfile.generic.GenericData.Record>) record.get(i);
                    List<Record> nrst = new ArrayList<Record>();
                    Schema ns = s.getFields().get(i).schema().getElementType();
                    for (neci.ncfile.generic.GenericData.Record nr : nrs) {
                        nrst.add(translate(nr, ns));
                    }
                    r.put(i, nrst);
                    break;
                }
                case FIXED:
                    break;
                case ENUM:
                    break;
                case NULL:
                    break;
                case UNION:
                    break;
                case MAP:
                    break;
            }
        }
        return r;
    }
}

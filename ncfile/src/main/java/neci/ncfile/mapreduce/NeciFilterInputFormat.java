package neci.ncfile.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.HadoopInput;

import neci.ncfile.FilterOperator;

public class NeciFilterInputFormat extends FileInputFormat<AvroWrapper<Record>, NullWritable> {
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    @Override
    protected FileStatus[] listStatus(JobConf job) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        job.setBoolean("mapred.input.dir.recursive", true);
        for (FileStatus file : super.listStatus(job))
            if (file.getPath().getName().endsWith(".neci"))
                result.add(file);
        return result.toArray(new FileStatus[0]);
    }

    @Override
    public RecordReader<AvroWrapper<Record>, NullWritable> getRecordReader(InputSplit split, final JobConf job,
            Reporter reporter) throws IOException {
        final FileSplit file = (FileSplit) split;
        reporter.setStatus(file.toString());
        job.setClass("filter", FilterOperator.class, FilterOperator.class);

        final AvroColumnReader.Params params = new AvroColumnReader.Params(new HadoopInput(file.getPath(), job));
        params.setModel(ReflectData.get());
        if (job.get(AvroJob.INPUT_SCHEMA) != null)
            params.setSchema(AvroJob.getInputSchema(job));

        return new RecordReader<AvroWrapper<Record>, NullWritable>() {
            private AvroColumnReader<Record> reader = new AvroColumnReader<Record>(params);
            private float rows = reader.getRowCount();
            private long row;

            public AvroWrapper<Record> createKey() {
                return new AvroWrapper<Record>(null);
            }

            public NullWritable createValue() {
                return NullWritable.get();
            }

            public boolean next(AvroWrapper<Record> wrapper, NullWritable ignore) throws IOException {
                if (!reader.hasNext())
                    return false;
                wrapper.datum(reader.next());
                row++;
                return true;
            }

            public float getProgress() throws IOException {
                return row / rows;
            }

            public long getPos() throws IOException {
                return row;
            }

            public void close() throws IOException {
                reader.close();
            }

        };

    }
}

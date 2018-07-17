package scan;

import java.io.File;
import java.io.IOException;
import java.util.List;

import neci.ncfile.ColumnReader;
import neci.ncfile.base.Schema;
import neci.ncfile.base.Schema.Field;
import neci.ncfile.generic.GenericData.Record;

public class FilterTest {
    public static void neciScan(String resultPath, List<Field> fs) throws IOException {
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        ColumnReader<Record> reader = new ColumnReader<Record>(result);
        for (int i = 0; i < fs.size(); i++) {
            int no = reader.getColumnNO(fs.get(i).name());
            while (reader.hasNext(no)) {
                String tt = reader.nextValue(no).toString();
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("##########neciScan\ntime: " + (end - start));
    }

    public static void neciCompare(String resultPath, List<Field> fs, String args) throws IOException {
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        ColumnReader<Record> reader = new ColumnReader<Record>(result);
        int[] mm = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            int no = reader.getColumnNO(fs.get(i).name());
            while (reader.hasNext(no)) {
                String tt = reader.nextValue(no).toString();
                if (tt.compareTo(args) < 0)
                    mm[i]++;
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("##########neciCompare\ntime: " + (end - start));
        for (int i = 0; i < mm.length; i++)
            System.out.println("mm: " + i + ": " + mm[i]);
    }

    public static void neciContains1(String resultPath, List<Field> fs, String args) throws IOException {
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        ColumnReader<Record> reader = new ColumnReader<Record>(result);
        int[] mm = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            int no = reader.getColumnNO(fs.get(i).name());
            while (reader.hasNext(no)) {
                String tt = reader.nextValue(no).toString();
                if (tt.contains(args))
                    mm[i]++;
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("##########neciContains1\ntime: " + (end - start));
        for (int i = 0; i < mm.length; i++)
            System.out.println("mm: " + i + ": " + mm[i]);
    }

    public static void neciContains2(String resultPath, List<Field> fs, String args) throws IOException {
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        ColumnReader<Record> reader = new ColumnReader<Record>(result);
        int[] mm = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            int no = reader.getColumnNO(fs.get(i).name());
            while (reader.hasNext(no)) {
                String tt = reader.nextValue(no).toString();
                String[] tmp = tt.split(" ");
                for (String t : tmp)
                    if (t.equals(args)) {
                        mm[i]++;
                        break;
                    }
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("##########neciContains2\ntime: " + (end - start));
        for (int i = 0; i < mm.length; i++)
            System.out.println("mm: " + i + ": " + mm[i]);
    }

    public static void neciMatches(String resultPath, List<Field> fs, String args) throws IOException {
        long start = System.currentTimeMillis();
        File result = new File(resultPath + "result.trv");
        ColumnReader<Record> reader = new ColumnReader<Record>(result);
        int[] mm = new int[fs.size()];
        for (int i = 0; i < fs.size(); i++) {
            int no = reader.getColumnNO(fs.get(i).name());
            while (reader.hasNext(no)) {
                String tt = reader.nextValue(no).toString();
                if (tt.matches(args))
                    mm[i]++;
            }
        }
        reader.close();
        long end = System.currentTimeMillis();
        System.out.println("##########neciMatches\ntime: " + (end - start));
        for (int i = 0; i < mm.length; i++)
            System.out.println("mm: " + i + ": " + mm[i]);
    }

    public static void main(String[] args) throws IOException {
        String resultPath = args[0];
        Schema s = new Schema.Parser().parse(new File(args[1]));
        List<Field> fs = s.getFields();
        if (args[2].compareTo("scan") == 0)
            neciScan(resultPath, fs);
        if (args[2].compareTo("compare") == 0)
            neciCompare(resultPath, fs, args[3]);
        if (args[2].compareTo("contain1") == 0)
            neciContains1(resultPath, fs, args[3]);
        if (args[2].compareTo("contain2") == 0)
            neciContains2(resultPath, fs, args[3]);
        if (args[2].compareTo("match") == 0)
            neciMatches(resultPath, fs, args[3]);
    }
}

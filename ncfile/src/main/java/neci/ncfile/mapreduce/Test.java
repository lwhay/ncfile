package neci.ncfile.mapreduce;

import org.apache.hadoop.conf.Configuration;

public class Test {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        Lfilter f = new Lfilter("t1", "t2");
        ConfigurationUtil.setClass("filter", conf, f);
        Lfilter filter = (Lfilter) ConfigurationUtil.getClass("filter", conf, Lfilter.class);
        System.out.println(filter.isMatch("t1"));
    }
}

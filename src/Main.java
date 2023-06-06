// import apache hadoop libraries
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.naming.Context;
import java.util.Iterator;

public class Main {
    // import csv file and map it to key value pairs

    public static class Map extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text word = new Text();
        private DoubleWritable val = new DoubleWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            word.set(line[0]);
            val.set(Double.parseDouble(line[1]));
            context.write(word, val);
        }
    }

    // reduce the key value pairs to get the average
    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            Iterator<DoubleWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                sum += iterator.next().get();
                count++;
            }
            result.set(sum/count);
            context.write(key, result);
        }
    }



    public static void main(String[] args) {
        System.out.println("Hello World!");
    }
}
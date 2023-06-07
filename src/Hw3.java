// import apache hadoop libraries
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Hw3 {
    public static class TotalMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable runtime = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // split csv file by comma, but ignore commas in quotes (e.g. "The, Movie")
            String[] fields = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            if (fields[14].equals("runtime")){
                return;
            }

            String movieTitle = fields[0];
            double rt;
            try {
                rt = Double.parseDouble(fields[14]);
            }
            catch (NumberFormatException e) {
                return;
            }
            int movieRuntime = (int) rt;
            runtime.set(movieRuntime);
            context.write(new Text("Total Runtime"), runtime);
        }
    }

    public static class TotalReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalRuntime = 0;
            for (IntWritable val : values) {
                totalRuntime += val.get();
            }
            result.set(totalRuntime);
            context.write(new Text(key), result);
        }
    }


    public static class AverageMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private final static DoubleWritable runtime = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            if (fields[14].equals("runtime")){
                return;
            }

            String movieTitle = fields[0];
            double movieRuntime;
            try {
                movieRuntime = Double.parseDouble(fields[14]);
            }
            catch (NumberFormatException e) {
                return;
            }
            runtime.set(movieRuntime);
            context.write(new Text("Average Runtime"), runtime);
        }
    }

    public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalRuntime = 0;
            int movieCount = 0;
            for (DoubleWritable val : values) {
                totalRuntime += val.get();
                movieCount++;
            }
            double averageRuntime = totalRuntime / movieCount;
            result.set(averageRuntime);
            context.write(new Text(key), result);
        }
    }


    public static class ActorMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            if (fields[9].equals("star")){
                return;
            }

            String actor = fields[9];
            context.write(new Text(actor), one);
        }
    }

    public static class ActorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int employmentCount = 0;
            for (IntWritable val : values) {
                employmentCount++;
            }
            result.set(employmentCount);
            context.write(key, result);
        }
    }


    public static class VotesMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable votes = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            if (fields[1].equals("rating")){
                return;
            }

            String rating = fields[1];
            if (rating.equals("G") || rating.equals("PG") || rating.equals("PG-13") || rating.equals("R")) {
                int voteCount = Integer.parseInt(fields[5]);
                votes.set(voteCount);
                context.write(new Text(rating), votes);
            }
        }
    }

    public static class VotesReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalVotes = 0;
            int movieCount = 0;
            for (IntWritable val : values) {
                totalVotes += val.get();
                movieCount++;
            }
            double averageVotes = (double) totalVotes / movieCount;
            result.set(averageVotes);
            context.write(key, result);
        }
    }


    public static class GenreMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private final static DoubleWritable score = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            if (fields[4].equals("genre")){
                return;
            }

            String genre = fields[4];
            double movieScore = Double.parseDouble(fields[6]);
            score.set(movieScore);
            context.write(new Text(genre), score);
        }
    }

    public static class GenreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalScore = 0;
            int movieCount = 0;
            for (DoubleWritable val : values) {
                totalScore += val.get();
                movieCount++;
            }
            double averageScore = totalScore / movieCount;
            result.set(averageScore);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws IOException {
        String type = args[0];

        if (type.equals("total")){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "total");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(TotalMapper.class);
            job.setReducerClass(TotalReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            try {
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        else if (type.equals("average")){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "average");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(AverageMapper.class);
            job.setReducerClass(AverageReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            try {
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        else if (type.equals("employment")){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "employment");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(ActorMapper.class);
            job.setReducerClass(ActorReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            try {
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        else if (type.equals("ratescore")){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "ratescore");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(VotesMapper.class);
            job.setReducerClass(VotesReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            try {
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        else if (type.equals("genrescore")){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "genrescore");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(GenreMapper.class);
            job.setReducerClass(GenreReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            try {
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            System.out.println("Invalid type");
        }
    }
}
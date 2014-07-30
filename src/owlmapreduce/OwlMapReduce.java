package owlmapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author phoenix
 */
public class OwlMapReduce {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String inputFileName = "/phoenix/biomed/input";
        String outputFileName = "/phoenix/biomed/output";
        
        FileSystem fileSystem = FileSystem.get(conf);
        Path outputpath = new Path(outputFileName);
        if (fileSystem.exists(outputpath)) {
            fileSystem.delete(outputpath, true);
        }

        Job job = new Job(conf, "Word Count");
        job.setJarByClass(OwlMapReduce.class);
        job.setMapperClass(OwlMapper.class);
        job.setReducerClass(OwlReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, outputpath);
        job.waitForCompletion(true);
    }

    public static class OwlMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Text outKey = new Text();
            IntWritable one = new IntWritable(1);
            for(String word: line.split(" ")){
                outKey.set(word);
                context.write(outKey, one);
            }
        }
    }
    
    public static class OwlReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int size = 0;
            for(IntWritable one: values){
                size++;
            }
            context.write(key, new IntWritable(size));
        }
        
    }

}

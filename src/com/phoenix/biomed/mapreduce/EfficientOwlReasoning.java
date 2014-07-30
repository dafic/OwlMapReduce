package com.phoenix.biomed.mapreduce;

import com.phoenix.biomed.common.Triple;
import com.phoenix.biomed.common.PCS;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.phoenix.biomed.main.Main;
/**
 *
 * @author phoenix
 */
public class EfficientOwlReasoning extends Configured implements Tool {

    String pcs = null;
    int level;

    public EfficientOwlReasoning(String pcs, int level) {
        this.pcs = pcs;
        this.level = level;
    }

    public static class EfficientOwlMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text outKey = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Triple triple = new Triple(value.toString());

            String pcsString = context.getConfiguration().get("pcs");
            System.out.println("pcs:" + pcsString);
            PCS pcs = new PCS(pcsString);

            int pid = pcs.getPID(triple.getPredicate());
            System.out.println("Pid:" + pid);
            if (pid == -1) {
                return;
            } else if (pid % 2 == 1) {
                outKey.set((pid - 1) + "_" + triple.getSubject());
            } else {
                outKey.set(pid + "_" + triple.getObject());
            }
            System.out.println("map output key:" + outKey);
            System.out.println("map output value:" + value);
            context.write(outKey, value);

        }

    }

    public static class EfficientOwlReducer extends Reducer<Text, Text, Text, Text> {

        Text outKey = new Text();
        Text outValue = new Text();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> subjectList = new ArrayList<String>();
            List<String> objectList = new ArrayList<String>();

            String pcsString = context.getConfiguration().get("pcs");
            PCS pcs = new PCS(pcsString);
            int len = pcs.size();

            String p0 = pcs.get0();
            String p1 = pcs.get1();

            boolean p0Present = false;
            boolean p1Present = false;

            int pid = -1;
            for (Text value : values) {
                Triple triple = new Triple(value.toString());
                pid = pcs.getPID(triple.getPredicate());

                if ((pid == (len - 1)) && (len % 2 == 1)) {
                    context.write(value, outValue);
                    return;
                }

                if (pid % 2 == 0) {
                    subjectList.add(triple.getSubject());
                } else {
                    objectList.add(triple.getObject());
                }
            }

            String newOpc = computeOPC(pcs, pid);
            for (String subject : subjectList) {
                for (String object : objectList) {
                    outKey.set(subject + ", " + newOpc + ", " + object);
                    context.write(outKey, outValue);
                    System.out.println(outKey);
                }
            }
        }

        private String computeOPC(PCS pcs, int pid) {
            String opc = pcs.get(pid);
            if (pid % 2 == 1) {
                opc = pcs.get(pid - 1) +"|"+ opc;
            } else {
                opc += "|"+pcs.get(pid + 1);
            }
            return opc;
        }

    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        conf.set("pcs", pcs);

        System.out.println("PCS:" + pcs);
        System.out.println("level:" + level);

        int outputLevel = level + 1;
        String inputFileName = "/phoenix/biomed/iinput" + level;
        String outputFileName = "/phoenix/biomed/iinput" + outputLevel;

        FileSystem fileSystem = FileSystem.get(conf);
        Path outputpath = new Path(outputFileName);
        if (fileSystem.exists(outputpath)) {
            fileSystem.delete(outputpath, true);
        }

        Job job = new Job(conf, "Efficient Owl Reasoning");
        job.setJarByClass(EfficientOwlReasoning.class);
        job.setMapperClass(EfficientOwlMapper.class);
        job.setReducerClass(EfficientOwlReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(inputFileName));
        FileOutputFormat.setOutputPath(job, outputpath);
        return job.waitForCompletion(true) ? 1 : 0;
    }
}

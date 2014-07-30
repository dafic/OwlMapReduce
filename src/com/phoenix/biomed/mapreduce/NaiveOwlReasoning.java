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
public class NaiveOwlReasoning extends Configured implements Tool {

    String pcs = null;
    int level;

    public NaiveOwlReasoning(String pcs, int level) {
        this.pcs = pcs;
        this.level = level;
    }

    public static class NaiveOwlMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text outKey = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Triple triple = new Triple(value.toString());
            String predicate = triple.getPredicate();

            String pcsString = context.getConfiguration().get("pcs");
            PCS pcs = new PCS(pcsString);
            String p0 = pcs.get0();
            String p1 = pcs.get1();

            System.out.println("p0:" + p0);
            System.out.println("p1:" + p1);

            if (predicate.equals(p0)) {
                System.out.println("equal p0:" + predicate);
                outKey.set(triple.getObject());
                context.write(outKey, value);
            } else if (predicate.equals(p1)) {
                System.out.println("equal p1:" + predicate);
                outKey.set(triple.getSubject());
                context.write(outKey, value);
            } else if (pcs.belongs(predicate)) {
                outKey.set("1");
                context.write(outKey, value);
            }
            System.out.println("map outKey:" + outKey);
            System.out.println("map outValue:" + value);
            

        }

    }

    public static class NaiveOwlReducer extends Reducer<Text, Text, Text, Text> {

        Text outKey = new Text();
        Text outValue = new Text();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> subjectList = new ArrayList<String>();
            List<String> objectList = new ArrayList<String>();

            String pcsString = context.getConfiguration().get("pcs");
            PCS pcs = new PCS(pcsString);
            String p0 = pcs.get0();
            String p1 = pcs.get1();

            boolean p0Present = false;
            boolean p1Present = false;

            for (Text value : values) {
                Triple triple = new Triple(value.toString());
                String predicate = triple.getPredicate();
                if (predicate.equals(p0)) {
                    subjectList.add(triple.getSubject());
                    p0Present = true;
                } else if (predicate.equals(p1)) {
                    objectList.add(triple.getObject());
                    p1Present = true;
                } else {
                    context.write(value, outValue);
                }
            }

            if (p0Present && p1Present) {
                String newOpc = p0 + "|" + p1;
                for (String subject : subjectList) {
                    for (String object : objectList) {
                        outKey.set(subject + ", " + newOpc + ", " + object);
                        context.write(outKey, outValue);
                    }
                }
            }
        }

    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
//        conf.set("p0", "treatment");
//        conf.set("p1", "possibleDrug");
//        conf.set("p2", "possibleDrug");
//        conf.set("p3", "possibleDrug");
//        conf.set("p4", "possibleDrug");
//        conf.set("p6", "possibleDrug");
//        conf.set("p7", "possibleDrug");

//        String pcs = "treatment, possibleDrug, hasTarget, hasAccession, classifiedWith, symbol";
        conf.set("pcs", pcs);
        System.out.println("PCS:"+pcs);
        System.out.println("new level:"+level);

        int outputLevel = level + 1;
        String inputFileName = "/phoenix/biomed/input" + level;
        String outputFileName = "/phoenix/biomed/input" + outputLevel;

        FileSystem fileSystem = FileSystem.get(conf);
        Path outputpath = new Path(outputFileName);
        if (fileSystem.exists(outputpath)) {
            fileSystem.delete(outputpath, true);
        }

        Job job = new Job(conf, "Naive Owl Reasoning");
        job.setJarByClass(NaiveOwlReasoning.class);
        job.setMapperClass(NaiveOwlMapper.class);
        job.setReducerClass(NaiveOwlReducer.class);
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

    public static void main(String[] args) {
        String PCS = "treatment, possibleDrug, hasTarget, hasAccession, classifiedWith, symbol";

        int maxLevel = 6;
        try {
            for (int level = 1; level < maxLevel; level++) {
                int pre1 = ToolRunner.run(new NaiveOwlReasoning(PCS, level), args);
                //update PCS
                PCS = updatePCS(PCS);
                System.out.println("PCS:"+PCS);
            }

        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static String updatePCS(String PCS) {
        String[] split = PCS.split(",");
        String newPCS = split[0].trim();
        if (split.length > 1) {
            newPCS += "|" + split[1].trim();
            // append the rest opc
            for (int i = 2; i < split.length; i++) {
                newPCS += ", " + split[i].trim();
            }
        }
        return newPCS;
    }
}

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;


public class PageRank {

    public static void main(String[] args) throws IOException {
        int numRepititions = 5;
        long leftover = 0;
        long size = 0;
        for(int i = 0; i < 2*numRepititions; i++) {
            Job job;
            if(i%2 == 0) {
                job = getTrustJob();
            }
            else {
                job = getLeftoverJob(leftover, size);
            }

            String inputPath = null;
            if (i == 0) {
                File[] inputFiles = new File("input/").listFiles();
                for (File inputFile : inputFiles) {
                    if (inputFile.getName().endsWith(".txt")) {
                        inputPath = inputFile.getPath();
                        System.out.println("Using input file " + inputPath);
                        break;
                    }
                }
                if (inputPath == null) {
                    System.err.println("No input file found! Exiting...");
                    return;
                }
            } else {
                inputPath = "stage" + (i-1);
            }
            String outputPath = "stage" + i;

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            try { 
                job.waitForCompletion(true);
            } catch(Exception e) {
                System.err.println("ERROR IN JOB: " + e);
                return;
            }
            if(i%2 == 0) {
                // Set up leftover and size
            } else {
                // Set up leftover and size
            }
                
        }
    }
    public static Job getStandardJob(String l, String s) throws IOException {
        Configuration conf = new Configuration();
        if(!l.equals("") && !s.equals("")) {
            conf.set("leftover", l);
            conf.set("size", s);
        }
        Job job = new Job(conf);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Node.class);

        job.setInputFormatClass(NodeInputFormat.class);
        job.setOutputFormatClass(NodeOutputFormat.class);

        job.setJarByClass(PageRank.class);

        return job;
    }

    public static Job getTrustJob() throws IOException{

        Job job = getStandardJob("", "");

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NodeOrDouble.class);
        
        job.setMapperClass(TrustMapper.class);
        job.setReducerClass(TrustReducer.class);

        return job;
    }

    public static Job getLeftoverJob(long l, long s) throws IOException{
        Job job = getStandardJob("" + l, "" + s);

        job.setMapperClass(LeftoverMapper.class);
        job.setReducerClass(LeftoverReducer.class);

        return job;
    }
}

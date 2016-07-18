package com.marklogic.mapreduce.test;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.marklogic.mapreduce.NodeInputFormat;
import com.marklogic.mapreduce.NodePath;
import com.marklogic.mapreduce.MarkLogicNode;

public class Test20772 {
    public static class MyMapper 
    extends Mapper<NodePath, MarkLogicNode, NodePath, MarkLogicNode> {
        public static final Log LOG =
            LogFactory.getLog(MyMapper.class);
        
        public void map(NodePath key, MarkLogicNode value, Context context) 
        throws IOException, InterruptedException {
            if (value != null) {
                context.write(key, value);
            } else {
                System.out.println("value is " + value);
            }
            
        }
    }
    
    public static class MyReducer
    extends Reducer<NodePath, MarkLogicNode, NodePath, MarkLogicNode> {
        
        public void reduce(NodePath key, Iterable<MarkLogicNode> nodes, 
            Context context) throws IOException, InterruptedException {        
            for (MarkLogicNode val : nodes) {
                System.out.println("key is " + key + 
                        ", val is \n" + val.toString());
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: Test20772 outputpath");
            System.exit(2);
        }

        Job job = new Job(conf);
        job.setJarByClass(Test20772.class);
        
        // Map related configuration
        job.setInputFormatClass(NodeInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(NodePath.class);
        job.setMapOutputValueClass(MarkLogicNode.class);
        job.setReducerClass(MyReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        conf.setInt("mapred.reduce.tasks", 0);

        conf = job.getConfiguration();
        conf.addResource(args[0]);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


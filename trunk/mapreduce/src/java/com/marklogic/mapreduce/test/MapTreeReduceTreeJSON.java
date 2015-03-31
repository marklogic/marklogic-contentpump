package com.marklogic.mapreduce.test;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DOMDocument;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.ForestDocument;
import com.marklogic.mapreduce.ForestInputFormat;
import com.marklogic.mapreduce.JSONDocument;
import com.marklogic.mapreduce.LargeBinaryDocument;
import com.marklogic.mapreduce.RegularBinaryDocument;

public class MapTreeReduceTreeJSON extends Configured implements Tool {
    public static class MyMapper 
    extends Mapper<DocumentURI, ForestDocument, DocumentURI, JSONDocument> {
        public static final Log LOG = LogFactory.getLog(MyMapper.class);
        
        public void map(DocumentURI key, JSONDocument value, Context context) 
        throws IOException, InterruptedException {
            if (value != null && value.getContentType()==ContentType.JSON) {
                System.out.println(key);
                System.out.println(value);
                context.write(key, value);
            } 
        }
    }
    
    public static class MyReducer
    extends Reducer<DocumentURI, JSONDocument, Text, Text> {
        Text keyText = new Text();
        Text valText = new Text();
        public void reduce(DocumentURI key, Iterable<ForestDocument> docs, 
            Context context) throws IOException, InterruptedException {        
            for (ForestDocument val : docs) {
                keyText.set(key.getUri());
                valText.set(val.getContentAsByteArray());
                context.write(keyText, valText);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: MapTreeReduceTree inputpath outputpath");
            System.exit(2);
        }
        Job job = new Job(super.getConf());
        job.setJarByClass(MapTreeReduceTreeJSON.class);
        
        // Map related configuration
        job.setInputFormatClass(ForestInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(DocumentURI.class);
        job.setMapOutputValueClass(JSONDocument.class);
        job.setReducerClass(MyReducer.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        return 0;
    }
    
    public static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, 
                new com.marklogic.mapreduce.test.MapTreeReduceTreeJSON(), args);
        System.exit(res);
      }
}

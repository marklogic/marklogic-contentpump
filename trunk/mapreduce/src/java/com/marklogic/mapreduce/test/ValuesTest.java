package com.marklogic.mapreduce.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.ValueInputFormat;
import com.marklogic.mapreduce.functions.PathReference;
import com.marklogic.mapreduce.functions.Reference;
import com.marklogic.mapreduce.functions.Values;

public class ValuesTest {
    public static class ValueMapper 
    extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            context.write(key, value);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: ValuesTest configFile outputDir");
            System.exit(2);
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(ValuesTest.class);
        job.setInputFormatClass(ValueInputFormat.class);
        job.setMapperClass(ValueMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        conf = job.getConfiguration();
        conf.addResource(otherArgs[0]);
        conf.setClass(MarkLogicConstants.INPUT_VALUE_CLASS, Text.class, 
                Writable.class);
        conf.setClass(MarkLogicConstants.INPUT_LEXICON_FUNCTION_CLASS, 
            ValuesFunction.class, Values.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    static class ValuesFunction extends Values {
        @Override
        public Reference[] getReferences() {
            PathReference pathRef = new MyPathReference();
            return new Reference[] {pathRef};
        }
        
    }
    
    static class MyPathReference extends PathReference {

        @Override
        public String getPathExpression() {
            return "/my:a[@his:b='B1']/my:c";
        }
        
    }
}

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
import com.marklogic.mapreduce.functions.ValueCooccurrences;

public class ValueCooccurrencesTest {
    public static class ValueCooccurrencesMapper 
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
            System.err.println("Usage: ValueCooccurrencesTest configFile outputDir");
            System.exit(2);
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(ValueCooccurrencesTest.class);
        job.setInputFormatClass(ValueInputFormat.class);
        job.setMapperClass(ValueCooccurrencesMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        conf = job.getConfiguration();
        conf.addResource(otherArgs[0]);
        conf.setClass(MarkLogicConstants.INPUT_VALUE_CLASS, Text.class, 
                Writable.class);
        conf.setClass(MarkLogicConstants.INPUT_LEXICON_FUNCTION_CLASS, 
            ValueCooccurrencesFunction.class, ValueCooccurrences.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    static class ValueCooccurrencesFunction extends ValueCooccurrences {
        
        @Override
        public String[] getUserDefinedOptions() {
            String[] options = 
                {"proximity=1"};
            return options;
        }

        @Override
        public Reference getReference1() {
            return new ItemPathReference();
        }

        @Override
        public Reference getReference2() {
            return new CityPathReference();
        }
    }
    
    static class ItemPathReference extends PathReference {

        @Override
        public String getPathExpression() {
            return "/news/sale/sold-item";
        }
        
    }
    
    static class CityPathReference extends PathReference {

        @Override
        public String getPathExpression() {
            return "/news/sale/city";
        }
        
    }
}

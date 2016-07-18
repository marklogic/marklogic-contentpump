/*
 * Copyright 2003-2015 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.KeyValueInputFormat;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.functions.ElemAttrValueCooccurrences;

/**
 * Count the occurrences of each link title in documents stored in MarkLogic
 * Server, using the lexicon function cts:element-attribute-co-occurrences, 
 * then write the link count summary to HDFS. Use with the
 * configuration file conf/marklogic-lexicon.xml.
 */
public class LinkCountCooccurrences {
    public static class RefMapper 
    extends Mapper<Text, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text refURI = new Text();

        public void map(Text key, Text value, Context context) 
        throws IOException, InterruptedException {
            String keyStr = key.toString();
            if (!keyStr.startsWith("#") && 
                !keyStr.startsWith("http://") &&
                !keyStr.startsWith("File:") &&
                !keyStr.startsWith("Image:") &&
                value != null) {
                refURI.set(value);
                context.write(refURI, one);
            }     
        }
    }
    
    public static class IntSumReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, 
                Context context
                ) throws IOException, InterruptedException {        
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println(
                    "Usage: LinkCountCooccurrences configFile outputDir");
            System.exit(2);
        }
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        Job job = new Job(conf);
        job.setJarByClass(LinkCountCooccurrences.class);
        job.setInputFormatClass(KeyValueInputFormat.class);
        job.setMapperClass(RefMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        conf = job.getConfiguration();
        conf.addResource(otherArgs[0]);
        conf.setClass(MarkLogicConstants.INPUT_KEY_CLASS, Text.class, 
                Writable.class);
        conf.setClass(MarkLogicConstants.INPUT_VALUE_CLASS, Text.class, 
                Writable.class);
        conf.setClass(MarkLogicConstants.INPUT_LEXICON_FUNCTION_CLASS, 
                HrefTitleMap.class, ElemAttrValueCooccurrences.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    static class HrefTitleMap extends ElemAttrValueCooccurrences {

        @Override
        public String getElementName1() {
            return "xs:QName(\"wp:a\")";
        }
        
        @Override
        public String getElementName2() {
            return "xs:QName(\"wp:a\")";
        }
        
        @Override
        public String getAttributeName1() {
            return "xs:QName(\"href\")";
        }
        
        @Override
        public String getAttributeName2() {
            return "xs:QName(\"title\")";
        }
        
        @Override
        public String[] getUserDefinedOptions() {
            String[] options = 
                {"collation=http://marklogic.com/collation/codepoint",
                 "proximity=0"};
            return options;
        }
        
        @Override
        public String getLexiconQuery() {
            return "cts:directory-query(\"enwiki/\", \"infinity\")";
        }
        
    }
}

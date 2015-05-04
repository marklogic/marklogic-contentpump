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
import java.util.List;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XPathSelector;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmItem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class LinkCountHDFS {
    public static class RefMapper 
    extends Mapper<IntWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text refURI = new Text();

        public void map(IntWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
            refURI.set(value);
            context.write(refURI, one);
        }
    }
    
    public static class IntSumReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, 
            Context context) throws IOException, InterruptedException {
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
            System.err.println("Usage: LinkCountHDFS inputDir outputDir");
            System.exit(2);
        }
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        Job job = Job.getInstance(conf, "link count hdfs");
        job.setJarByClass(LinkCountHDFS.class);
        job.setInputFormatClass(HDFSInputFormat.class);
        job.setMapperClass(RefMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setReducerClass(IntSumReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        HDFSInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class HDFSInputFormat extends FileInputFormat<IntWritable, Text> {
    
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<IntWritable, Text> createRecordReader(InputSplit arg0,
            TaskAttemptContext arg1) throws IOException, InterruptedException {
        return new LinkRecordReader();
    }
}

class LinkRecordReader extends RecordReader<IntWritable, Text> {
    private static final String PATH_EXPRESSION = 
        "//wp:a[@title and @href and not (starts-with(@href, '#') or " +
        "starts-with(@href, 'http://') or starts-with(@href, 'File:') " +
        " or starts-with(@href, 'Image:'))]/@title";
    private IntWritable key;
    private Text value;
    private int count = 0;
    private List<XdmItem> items;
    
    private static final ThreadLocal<DocumentBuilder> builderLocal =  
        new ThreadLocal<DocumentBuilder>() {          
        @Override protected DocumentBuilder initialValue() { 
            try { 
                return  
                DocumentBuilderFactory.newInstance().newDocumentBuilder(); 
            } catch (ParserConfigurationException e) { 
                e.printStackTrace();
                return null; 
            }  
        }      
    };  
    
    private static Processor proc = new Processor(false);
    
    private static final ThreadLocal<net.sf.saxon.s9api.DocumentBuilder> 
        saxonBuilderLocal =
            new ThreadLocal<net.sf.saxon.s9api.DocumentBuilder>() {
        @Override protected net.sf.saxon.s9api.DocumentBuilder initialValue() {
            return proc.newDocumentBuilder();
        }
    };
                 
    @Override
    public void close() throws IOException {
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (items != null) {
            return (float)count/items.size();
        } else {
            return 0;
        }
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Path file = ((FileSplit)inSplit).getPath();
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);
        DocumentBuilder docBuilder = builderLocal.get();
        try {
            Document document = docBuilder.parse(fileIn);
            net.sf.saxon.s9api.DocumentBuilder db = saxonBuilderLocal.get();
            XdmNode xdmDoc = db.wrap(document);
            XPathCompiler xpath = proc.newXPathCompiler();
            xpath.declareNamespace("wp", 
                    "http://www.mediawiki.org/xml/export-0.4/");
            XPathSelector selector = xpath.compile(PATH_EXPRESSION).load();
            selector.setContextItem(xdmDoc);
            items = new ArrayList<XdmItem>();
            for (XdmItem item : selector) {
                items.add(item);
            }
        } catch (SAXException ex) {
            ex.printStackTrace();
            throw new IOException(ex);
        } catch (SaxonApiException e) {
            e.printStackTrace();
        } finally {
            if (fileIn != null) {
                fileIn.close();
            }
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (items.size() > count) {
            if (key == null) {
                key = new IntWritable();
            }
            key.set(count);
            if (value == null) {
                value = new Text();
            }
            value.set(items.get(count++).getStringValue());
            return true;
        }
        return false;
    }
    
}

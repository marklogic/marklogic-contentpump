/*
 * Copyright 2003-2016 MarkLogic Corporation
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
import java.util.Collections;
import java.util.ArrayList;
import java.util.Iterator;

import org.w3c.dom.Document;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DatabaseDocument;
import com.marklogic.mapreduce.DocumentInputFormat;
import com.marklogic.mapreduce.DocumentURI;
import java.util.List;

/**
 * Read the first word from each input document, then produce
 * a single output document containing the words, sorted, and concatenated
 * into a single string. Only XML documents with text nodes contribute
 * to the final result.
 *
 * <p>This sample uses the marklogic-hello-world.xml config file.</p>
 * <p>
 *  The config file name is hard-coded into the sample for simplicity, 
 *  so no additional command line options are required by the sample.
 * </p>
 * <p>
 *   The mapper creates key-value pairs where the key is always the same
 *   constant and the value is the first word from the document. The reducer
 *   sorts the words, concatenates them together, and writes them to an
 *   output document. Since all key-value pairs produced by the mapper have
 *   the same key, there's only one input pair to the reducer, producing  a
 *   a single output document.
 * </p>
 * <p>
 *   For example, given 2 input documents whose first words are "hello" and
 *   "world", the mapper produces: (1, "hello") and (1, "world"). The reducer
 *   receives (1, ("hello", "world")) as input and inserts HelloWorld.txt
 *   in the database, containing "hello world".
 * </p>
 */
public class HelloWorld {
    public static class MyMapper 
    extends Mapper<DocumentURI, DatabaseDocument, IntWritable, Text> {
        public static final Log LOG =
            LogFactory.getLog(MyMapper.class);
        private final static IntWritable one = new IntWritable(1);
        private Text firstWord = new Text();
        
        @Override
        public void map(DocumentURI key, DatabaseDocument value, Context context) 
        throws IOException, InterruptedException {
        	if (key != null && value != null && value.getContentSize() != 0) {
        		ContentType contentType = value.getContentType();
        		if (contentType == ContentType.XML) {
        			// grab the first word from the document text
                    Document doc = (Document)value.getContentAsMarkLogicNode().get();
                    String text = doc.getDocumentElement().getTextContent();
                    firstWord.set(text.split(" ", 2)[0]);
                    context.write(one, firstWord);
        		}      		                
            } else {
                LOG.error("key: " + key + ", value: " + value);
            }
        }
    }
    
    public static class MyReducer
    extends Reducer<IntWritable, Text, DocumentURI, Text> {
        public static final Log LOG =
            LogFactory.getLog(MyMapper.class);
        private Text result = new Text();
        private static final DocumentURI outputURI = 
            new DocumentURI("HelloWorld.txt");
        private String allWords = new String();
        
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, 
                Context context
                ) throws IOException, InterruptedException {        
            // Sort the words
            List<String> words = new ArrayList<>();
            for (Text val : values) {
                words.add(val.toString());
            }
            Collections.sort(words);
            
            // concatenate the sorted words into a single string
            allWords = "";
            Iterator<String> iter = words.iterator();
            while (iter.hasNext()) {
                allWords += iter.next() + " ";
            }
            
            // save the final result
            result.set(allWords.trim());
            context.write(outputURI, result);

        }
    }

    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf, "hello world");
        job.setJarByClass(HelloWorld.class);
        
        // Map related configuration
        job.setInputFormatClass(DocumentInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        // Reduce related configuration
        job.setReducerClass(MyReducer.class);
        job.setOutputFormatClass(ContentOutputFormat.class);
        job.setOutputKeyClass(DocumentURI.class);
        job.setOutputValueClass(Text.class);

        conf = job.getConfiguration();
        conf.addResource("marklogic-hello-world.xml");
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

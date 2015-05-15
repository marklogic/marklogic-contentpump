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
package com.marklogic.mapreduce.test;

import java.io.IOException;

import javax.xml.soap.Node;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.marklogic.mapreduce.NodeInputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicNode;
import com.marklogic.mapreduce.NodePath;
import com.marklogic.mapreduce.KeyValueOutputFormat;

/**
 * Use custom query to get documents from MarkLogic Server,
 * and aggregate them together as an archive document, 
 * then use custom query to save it into MarkLogic Server.
 * Use with the configuration file conf/marklogic-qryin-qryout.xml.
 * 
 * @author mattsun
 *
 */
public class CustomQuery {
    public static class QueryMapper
    extends Mapper<NodePath, MarkLogicNode, IntWritable, Text> {
        public static final Log LOG =
                LogFactory.getLog(QueryMapper.class);
        private final static IntWritable one = new IntWritable(1);
        private Text docStr = new Text();
        
        public void map (NodePath key, MarkLogicNode value, Context context)
        throws IOException, InterruptedException {
            if (key != null && value != null && value.get() != null){
                if (value.get().getNodeType() == Node.DOCUMENT_NODE) {
                    String text = removeHeader(value.toString());
                    System.out.println(text);
                    docStr.set(text);
                    context.write(one, docStr);
                }
            }
            else {
                LOG.error("key: " + key + ", value: " + value);
            }
        }
        
        private String removeHeader(String s) {
            // remove the header "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
            int index = s.indexOf(">");
            if (index == -1) return s;
            else return s.substring(index+1);
        }
    }
    
    public static class QueryReducer 
    extends Reducer<IntWritable, Text, Text, Text>{
        public static final Log LOG =
                LogFactory.getLog(QueryReducer.class);
        private Text uri = new Text();
        private Text content = new Text();
        
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            StringBuilder buf = new StringBuilder();
            buf.append("<archives>");
            for (Text val : values) {
                buf.append("<archive>\n").append(val).append("</archive>\n");
            }
            buf.append("</archives>");
            content.set(buf.toString());
            uri.set("/queryReducer/example/archive");
            context.write(uri, content);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 1) {
            System.err.println("Usage: CustomQuery configFile");
            System.exit(2);
        }
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "custom query");
        job.setJarByClass(CustomQuery.class);
        
        job.setInputFormatClass(NodeInputFormat.class);
        job.setMapperClass(QueryMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(QueryReducer.class);
        job.setOutputFormatClass(KeyValueOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        conf = job.getConfiguration();
        conf.addResource(otherArgs[0]);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

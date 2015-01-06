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
import java.io.InputStream;
import java.io.StringBufferInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.marklogic.mapreduce.MarkLogicNode;
import com.marklogic.mapreduce.NodeInputFormat;
import com.marklogic.mapreduce.NodeOutputFormat;
import com.marklogic.mapreduce.NodePath;

/**
 * Count the number of occurrences of each link title in documents in
 * MarkLogic Server, and save the link count as a child node of each
 * referenced document. Use with the configuration file 
 * conf/marklogic-nodein-nodeout.xml.
 */
@SuppressWarnings("deprecation")
public class LinkCountInDoc {
    public static class RefMapper 
    extends Mapper<NodePath, MarkLogicNode, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text refURI = new Text();
        
        public void map(NodePath key, MarkLogicNode value, Context context) 
        throws IOException, InterruptedException {
            if (value != null && value.get() != null) {
                Attr title = (Attr)value.get();
                String href = title.getValue();
                
                refURI.set(href.trim());
                context.write(refURI, one);
            } 
        }
    }
    
    public static class IntSumReducer
    extends Reducer<Text, IntWritable, NodePath, MarkLogicNode> {
        private final static String TEMPLATE = "<ref-count>0</ref-count>";
        private final static String ROOT_ELEMENT_NAME = "//wp:page";
        private final static String BASE_URI_PARAM_NAME = "mapreduce.linkcount.baseuri";
        
        private NodePath nodePath = new NodePath();
        private Element element;
        private MarkLogicNode result;
        private String baseUri;
        
        protected void setup(Context context) 
        throws IOException, InterruptedException {
            try {
                DocumentBuilder docBuilder = 
                    DocumentBuilderFactory.newInstance().newDocumentBuilder();
                InputStream sbis = new StringBufferInputStream(TEMPLATE);
                element = docBuilder.parse(sbis).getDocumentElement();
                result = new MarkLogicNode(element);
                baseUri = context.getConfiguration().get(BASE_URI_PARAM_NAME);
            } catch (ParserConfigurationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (SAXException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        public void reduce(Text key, Iterable<IntWritable> values, 
                Context context
                ) throws IOException, InterruptedException {        
            int sum = 0;
            if (isInvalidName(key)) {
                return;
            }
            for (IntWritable val : values) {
                sum += val.get();
            }
            StringBuilder buf = new StringBuilder();
            buf.append(baseUri).append(key);
            nodePath.setDocumentUri(buf.toString());
            nodePath.setRelativePath(ROOT_ELEMENT_NAME);
            element.setTextContent(Integer.toString(sum));
            context.write(nodePath, result);
        }
        
        // exclude key that is invalid for a document URI
        private boolean isInvalidName(Text key) {
            String keyString = key.toString();
            return keyString == null || keyString.isEmpty() || 
                    keyString.matches("( )*");
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 1) {
            System.err.println("Usage: LinkCountInDoc configFile");
            System.exit(2);
        }

        Job job = new Job(conf);
        job.setJarByClass(LinkCountInDoc.class);
        job.setInputFormatClass(NodeInputFormat.class);
        job.setMapperClass(RefMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputFormatClass(NodeOutputFormat.class);
        job.setOutputKeyClass(NodePath.class);
        job.setOutputValueClass(MarkLogicNode.class);
        
        conf = job.getConfiguration();
        conf.addResource(args[0]);
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



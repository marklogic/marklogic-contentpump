/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Element;

import com.marklogic.mapreduce.KeyValueOutputFormat;
import com.marklogic.mapreduce.MarkLogicNode;
import com.marklogic.mapreduce.NodeInputFormat;
import com.marklogic.mapreduce.NodePath;

/**
 * Extract the revision year from the document and add that as a collection to 
 * the document.  This example demonstrates how to use NodeInputFormat and
 * KeyValueOutputFormat, and can be run with the configuration file 
 * conf/marklogic-nodein-qryout.xml.
 *
 */
public class RevisionGrouper {
    public static class RevisionMapper 
    extends Mapper<NodePath, MarkLogicNode, Text, Text> {
        private Text uri = new Text();
        private Text year = new Text();

        public void map(NodePath key, MarkLogicNode value, Context context) 
        throws IOException, InterruptedException {
            if (value != null && value.get() != null) {
                Element ts = (Element)value.get();
                year.set(ts.getTextContent().split("-")[0]);
                uri.set(key.getDocumentUri());
                
                context.write(uri, year);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 1) {
            System.err.println("Usage: RevisionGrouper configFile");
            System.exit(2);
        }

        Job job = new Job(conf);
        job.setJarByClass(RevisionGrouper.class);
        job.setInputFormatClass(NodeInputFormat.class);
        job.setMapperClass(RevisionMapper.class);
     
        job.setOutputFormatClass(KeyValueOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        conf = job.getConfiguration();
        conf.addResource(args[0]);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

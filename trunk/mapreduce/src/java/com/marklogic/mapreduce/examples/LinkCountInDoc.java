/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.marklogic.mapreduce.MarkLogicNode;
import com.marklogic.mapreduce.NodeInputFormat;
import com.marklogic.mapreduce.NodeOutputFormat;
import com.marklogic.mapreduce.NodePath;

@SuppressWarnings("deprecation")
public class LinkCountInDoc {
	public static class RefMapper 
	extends Mapper<NodePath, MarkLogicNode, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private final static String TITLE_ATTR_NAME = "title";
		private Text refURI = new Text();
		
		public void map(NodePath key, MarkLogicNode value, Context context) 
		throws IOException, InterruptedException {
			Element element = (Element)value.getNode();
			String href = element.getAttribute(TITLE_ATTR_NAME);
			
			refURI.set(href);
			context.write(refURI, one);
			
			// TODO: if the base URI needs to be extracted from the key, 
			// do it here.
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
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
			System.err.println("Usage: OutputTestMR configFile");
			System.exit(2);
		}

		Job job = new Job(conf);
		job.setJarByClass(LinkCount.class);
		job.setInputFormatClass(NodeInputFormat.class);
		job.setMapperClass(RefMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputFormatClass(NodeOutputFormat.class);
		job.setOutputKeyClass(NodePath.class);
	    job.setOutputValueClass(MarkLogicNode.class);
		
		conf = job.getConfiguration();
		conf.addResource(otherArgs[0]);
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}



package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * MarkLogicInputFormat for Node.
 * 
 * @author jchen
 */
public class NodeInputFormat extends MarkLogicInputFormat<NodePath> {
    
	static final float NODE_TO_FRAGMENT_RATIO = 100;
	
	@Override
	public RecordReader<NodePath, MarkLogicRecord> createRecordReader(
			InputSplit arg0, TaskAttemptContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String serverUri = getServerUriTemp(conf);
		String pathExpr = conf.get(PATH_EXPRESSION, "");
		String nameSpace = conf.get(PATH_NAMESPACE, "");
		return new NodeReader(serverUri, pathExpr, nameSpace);
	}

	@Override
	public float getDefaultRecordFragRatio() {
		return NODE_TO_FRAGMENT_RATIO;
	}

}

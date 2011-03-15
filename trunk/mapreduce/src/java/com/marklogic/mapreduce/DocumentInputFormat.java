package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * MarkLogicInputFormat for Document.
 * 
 * @author jchen
 */
public class DocumentInputFormat extends MarkLogicInputFormat<DocumentURI> {

	static final float DOCUMENT_TO_FRAGMENT_RATIO = 1;
	
	@Override
	public RecordReader<DocumentURI, MarkLogicRecord> createRecordReader(
			InputSplit arg0, TaskAttemptContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String serverUri = getServerUriTemp(conf);
		String pathExpr = conf.get(PATH_EXPRESSION, "");
		String nameSpace = conf.get(PATH_NAMESPACE, "");
   
		return new DocumentReader(serverUri, pathExpr, nameSpace);
	}
	
	@Override
	public float getDefaultRecordFragRatio() {
		return DOCUMENT_TO_FRAGMENT_RATIO;
	}

}

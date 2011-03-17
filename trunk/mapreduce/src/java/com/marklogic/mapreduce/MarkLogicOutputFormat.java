package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;

/**
 * MarkLogic-based OutputFormat.
 * 
 * @author jchen
 */
public abstract class MarkLogicOutputFormat<K> extends OutputFormat<K, MarkLogicNode> 
implements MarkLogicConstants {
	public static final Log LOG =
	    LogFactory.getLog(MarkLogicOutputFormat.class);
	
	static final String DIRECTORY_TEMPLATE = "{dir}";
	static final String DELETE_DIRECTORY_TEMPLATE = 
		"xdmp:directory-delete(\"" + DIRECTORY_TEMPLATE + "\")";
	static final String CHECK_DIRECTORY_EXIST_TEMPLATE = 
		"exists(xdmp:document-properties(\"" + DIRECTORY_TEMPLATE + 
		"\")//prop:directory)";
	
	/**
     * get server URI from the configuration.
     * 
     * @param conf job configuration
     * @return server URI
     * @throws URISyntaxException 
     */
    protected URI getServerUri(Configuration conf) throws URISyntaxException {
		String user = conf.get(OUTPUT_USERNAME, "");
		String password = conf.get(OUTPUT_PASSWORD, "");
		String host = conf.get(OUTPUT_HOST, "");
		String port = conf.get(OUTPUT_PORT, "");
		
		String serverUriStr = 
			SERVER_URI_TEMPLATE.replace(USER_TEMPLATE, user)
			.replace(PASSWORD_TEMPLATE, password)
			.replace(HOST_TEMPLATE, host)
			.replace(PORT_TEMPLATE, port);
		return new URI(serverUriStr);
    }
    
	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		Session session = null;
		ResultSequence result = null;
		try {
			URI serverUri = getServerUri(conf);
		    // try getting a connection
			ContentSource cs = ContentSourceFactory.newContentSource(serverUri);
			session = cs.newSession();
			
			String outputDir = conf.get(OUTPUT_DIRECTORY);
			if (conf.getBoolean(OUTPUT_CLEAN_DIR, false)) { 
				// delete directory if exists
				String queryText = DELETE_DIRECTORY_TEMPLATE.replace(
						DIRECTORY_TEMPLATE, outputDir);
				AdhocQuery query = session.newAdhocQuery(queryText);
				result = session.submitRequest(query);
			} else {
				// make sure the output directory doesn't exist
				String queryText = 
					CHECK_DIRECTORY_EXIST_TEMPLATE.replace(
						DIRECTORY_TEMPLATE, outputDir);
				AdhocQuery query = session.newAdhocQuery(queryText);
				result = session.submitRequest(query);
				if (result.hasNext()) {
					ResultItem item = result.next();
					if (item.getItem().asString().equals("true")) {
						throw new IOException("Directory " + outputDir + 
								" already exists");
					}
				}
			}
		} catch (URISyntaxException e) {
			LOG.error(e);
			throw new IOException(e);
		} catch (XccConfigException e) {
			LOG.error(e);
			throw new IOException(e);
		} catch (RequestException e) {
			LOG.error(e);
		} finally {
			if (result != null) {
				result.close();
			}
			if (session != null) {
				session.close();
			}
		}	
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
		        context);
	}
}

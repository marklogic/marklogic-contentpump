package com.marklogic.mapreduce;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

/**
 * MarkLogicRecordWriter that inserts content in bytes to MarkLogicServer.
 * 
 * @author jchen
 *
 */
public class ContentWriter<VALUEOUT> 
extends MarkLogicRecordWriter<DocumentURI, VALUEOUT> implements MarkLogicConstants {
	public static final Log LOG = LogFactory.getLog(ContentWriter.class);
	
	/**
	 * Directory of the output documents.
	 */
	private String outputDir;
	
	/**
	 * Content options of the output documents.
	 */
	private ContentCreateOptions options;
	
	/**
	 * A map from a forestId to a ContentSource. 
	 */
	private Map<String, ContentSource> forestSourceMap;
	
	/**
	 * An array of forest ids
	 */
	private String[] forestIds;

	public ContentWriter(Configuration conf, 
			Map<String, ContentSource> forestSourceMap) {
		super(null, conf);
		
		this.forestSourceMap = forestSourceMap;
		forestIds = new String[forestSourceMap.size()];
		forestIds = forestSourceMap.keySet().toArray(forestIds);
		
		this.outputDir = conf.get(OUTPUT_DIRECTORY);
		
		String[] perms = conf.getStrings(OUTPUT_PERMISSION);
		ContentPermission[] permissions = null;
		if (perms != null && perms.length > 0) {
	    	permissions = new ContentPermission[perms.length / 2];
	    	int i = 0;
	    	while (i + 1 < perms.length) {
	    		String roleName = perms[i++];
	    		String perm = perms[i];
	    		ContentCapability capability = null;
	    		if (perm.equalsIgnoreCase(ContentCapability.READ.toString())) {
	    			capability = ContentCapability.READ;
	    		} else if (perm.equalsIgnoreCase(ContentCapability.EXECUTE.toString())) {
	    			capability = ContentCapability.EXECUTE;
	    		} else if (perm.equalsIgnoreCase(ContentCapability.INSERT.toString())) {
	    			capability = ContentCapability.INSERT;
	    		} else if (perm.equalsIgnoreCase(ContentCapability.UPDATE.toString())) {
	    			capability = ContentCapability.UPDATE;
	    		} else {
	    			LOG.error("Illegal permission: " + perm);
	    		}
	    		permissions[i/2] = new ContentPermission(capability, roleName);
	    		i++;
	    	}
		} else {
	    	LOG.debug("no permissions");
		}
		
		options = ContentCreateOptions.newXmlInstance();
	    options.setCollections(conf.getStrings(OUTPUT_COLLECTION));
	    options.setQuality(conf.getInt(OUTPUT_QUALITY, 0));
	    options.setPermissions(permissions);
	    String contentTypeStr = conf.get(CONTENT_TYPE, DEFAULT_CONTENT_TYPE);
	    ContentType contentType = ContentType.valueOf(contentTypeStr);
	    options.setFormat(contentType.getDocumentFormat());
    }

	@Override
    public void write(DocumentURI key, VALUEOUT value) 
	throws IOException, InterruptedException {
		long hashCode = (long)key.getUri().hashCode() + Integer.MAX_VALUE;
		int fId = (int)(hashCode % forestSourceMap.size());
		String forestId = forestIds[fId];
		ContentSource cs = forestSourceMap.get(forestId);
		Session session = null;
		try {
			session = cs.newSession(forestId);
			String uri = key.getUri();
			if (outputDir != null && !outputDir.isEmpty()) {
			    uri = outputDir.endsWith("/") || uri.startsWith("/") ? 
			    	  outputDir + uri : outputDir + '/' + uri;
			}	
			Content content = null;
			if (value instanceof Text) {
				content = ContentFactory.newContent(uri, 
			    		((Text) value).toString(), options);
			} else if (value instanceof MarkLogicNode) {
				content = ContentFactory.newContent(uri, 
			    		((MarkLogicNode)value).get(), options);	    
			} else {
				throw new UnsupportedOperationException(value.getClass() + 
						" is not supported.");
			}		
			session.insertContent(content);
			commitIfNecessary();
		} catch (RequestException e) {
			LOG.error(e);
			throw new IOException(e);
		} finally {
			try {
				if (session != null) {
	                session.close();
				}
            } catch (RequestException e) {
	            LOG.error("Error closing session", e);
            }
		}
	    
    }
}

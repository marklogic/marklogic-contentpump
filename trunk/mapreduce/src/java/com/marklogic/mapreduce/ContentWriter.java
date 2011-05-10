package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentPermission;
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
	//private List<Long> forestIds;
	int index = 0;

	public ContentWriter(URI serverUri, Configuration conf) {
		super(serverUri, conf);
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
	    /* TODO: set option to do in-forest eval when 13333 is fixed.
	     * long[] forests = new long[forestIds.size()];
	    for (int i = 0; i < forestIds.size(); i++) {
	    	forests[i] = forestIds.get(i);
	    }
	    options.setPlaceKeys(forests); */
    }

	@Override
    public void write(DocumentURI key, VALUEOUT value) 
	throws IOException, InterruptedException {
		Session session = getSession();
		try {
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
		}
	    
    }
}

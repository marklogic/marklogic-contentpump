package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
extends MarkLogicRecordWriter<DocumentURI, VALUEOUT> {
	public static final Log LOG = LogFactory.getLog(ContentWriter.class);
	/**
	 * Directory of the output documents.
	 */
	private String outputDir;
	/**
	 * Content options of the output documents.
	 */
	private ContentCreateOptions options;

	public ContentWriter(URI serverUri, String outputDir, String[] collections,
            String[] perms, String quality) {
		super(serverUri);
		this.outputDir = outputDir;
		
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
	    options.setCollections(collections);
            if (quality != null) {
	        options.setQuality(Integer.parseInt(quality));
            }
	    options.setPermissions(permissions);
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
			    		((Text) value).getBytes(), options);
			} else if (value instanceof MarkLogicNode) {
				content = ContentFactory.newContent(uri, 
			    		((MarkLogicNode)value).getNode(), options);	    
			} else {
				throw new UnsupportedOperationException(value.getClass() + 
						" is not supported.");
			}		
			session.insertContent(content);
		} catch (RequestException e) {
			LOG.error(e);
			throw new IOException(e);
		}
	    
    }
}

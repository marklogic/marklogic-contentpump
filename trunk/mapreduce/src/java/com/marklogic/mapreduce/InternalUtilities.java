package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.exceptions.XccConfigException;

/**
 * Internal utilities shared by the package.  No need to document.
 * 
 * @author jchen
 */
public class InternalUtilities implements MarkLogicConstants {

	/**
	 * Get input server URI based on the job configuration.
	 * @param conf job configuration
	 * @return server URI
	 * @throws URISyntaxException
	 */
	static URI getInputServerUri(Configuration conf) throws URISyntaxException {
		String host = conf.get(INPUT_HOST, "");
		return getInputServerUri(conf, host);
    }
	
	/**
	 * Get input server URI based on the job configuration and server host 
	 * name.
	 * @param conf job configuration
	 * @param hostName name of targeted input server host
	 * @return server URI
	 * @throws URISyntaxException
	 */
	static URI getInputServerUri(Configuration conf, String hostName) 
	throws URISyntaxException {
		String user = conf.get(INPUT_USERNAME, "");
		String password = conf.get(INPUT_PASSWORD, "");
		String port = conf.get(INPUT_PORT, "");
		boolean useSsl = conf.getBoolean(INPUT_USE_SSL, false);
		return getServerUri(user, password, hostName, port, useSsl);
	}
	
	/**
	 * Get output server URI based on the job configuration and server host 
	 * name.
	 * @param conf job configuration
	 * @param hostName name of targeted output server host
	 * @return server URI
	 * @throws URISyntaxException
	 */
	static URI getOutputServerUri(Configuration conf, String hostName)
	throws URISyntaxException {
		String user = conf.get(OUTPUT_USERNAME, "");
		String password = conf.get(OUTPUT_PASSWORD, "");
		String port = conf.get(OUTPUT_PORT, "");
		boolean useSsl = conf.getBoolean(OUTPUT_USE_SSL, false);
		return getServerUri(user, password, hostName, port, useSsl);
	}
	
	static URI getServerUri(String user, String password, String hostName, 
			String port, boolean useSsl) throws URISyntaxException {		
		StringBuilder buf = new StringBuilder();
		if (useSsl) {
			buf.append("xccs://");
		} else {
			buf.append("xcc://");
		}
		buf.append(user);
		buf.append(":");
		buf.append(password);
		buf.append("@");
		buf.append(hostName);
		buf.append(":");
		buf.append(port);
		
		return new URI(buf.toString());
	}
	
	/**
	 * Get content source for input server.
	 * @param conf job configuration.
	 * @return ContentSource for input server.
	 * @throws URISyntaxException
	 * @throws XccConfigException
	 * @throws IOException
	 */
	static ContentSource getInputContentSource(Configuration conf) 
	throws URISyntaxException, XccConfigException, IOException {
		return getInputContentSource(conf, getInputServerUri(conf));
	}
	
	/**
     * Get input content source.
     *
     * @param conf job configuration
     * @param serverUri server URI
     * @return content source
	 * @throws IOException 
     * @throws XccConfigException
     * @throws URISyntaxException 
     * @throws IOException 
     */
    static ContentSource getInputContentSource(Configuration conf, 
    		URI serverUri)
	throws IOException, XccConfigException { 
    	boolean useSsl = conf.getBoolean(INPUT_USE_SSL, false);
    	if (!useSsl) {
    		return ContentSourceFactory.newContentSource(serverUri);
    	} else {
    		Class<? extends SslConfigOptions> sslOptionClass = 
    			conf.getClass(INPUT_SSL_OPTIONS_CLASS, 
				null, SslConfigOptions.class);
    	    if (sslOptionClass != null) {
    	        SslConfigOptions sslOptions = 
				    (SslConfigOptions)DefaultStringifier.load(conf, 
						INPUT_SSL_OPTIONS, sslOptionClass);
    	        return getSecureContentSource(serverUri, sslOptions);
    	    } else {
    	    	return ContentSourceFactory.newContentSource(serverUri);
    	    }
    	}
	}
	
	/**
     * Get output content source.
     *
     * @param conf job configuration
     * @param serverUri server URI
     * @return content source
	 * @throws IOException 
     * @throws XccConfigException
     * @throws URISyntaxException 
     * @throws IOException 
     */
	static ContentSource getOutputContentSource(Configuration conf,
			URI serverUri)
	throws IOException, XccConfigException { 
    	boolean useSsl = conf.getBoolean(OUTPUT_USE_SSL, false);
    	if (!useSsl) {
    		return ContentSourceFactory.newContentSource(serverUri);
    	} else {
    	    Class<? extends SslConfigOptions> sslOptionClass = 
    	    	conf.getClass(OUTPUT_SSL_OPTIONS_CLASS, 
				null, SslConfigOptions.class);
    	    if (sslOptionClass != null) {
    	        SslConfigOptions sslOptions = 
				    (SslConfigOptions)DefaultStringifier.load(conf, 
						OUTPUT_SSL_OPTIONS, sslOptionClass);
    	        return getSecureContentSource(serverUri, sslOptions);
    	    } else {
    	    	return ContentSourceFactory.newContentSource(serverUri);
    	    }
    	}
	}
	
	static ContentSource getSecureContentSource(URI serverUri, 
			SslConfigOptions sslOptions) throws XccConfigException {
    	ContentSource contentSource = null;
      
		// construct XCC SecurityOptions
		SecurityOptions options = 
			new SecurityOptions(sslOptions.getSslContext());
		options.setEnabledProtocols(sslOptions.getEnabledCipherSuites());
	    options.setEnabledProtocols(sslOptions.getEnabledProtocols());
  
	    // construct content source
		contentSource = ContentSourceFactory.newContentSource(
				serverUri, options);		
 
    	return contentSource;
	}
}
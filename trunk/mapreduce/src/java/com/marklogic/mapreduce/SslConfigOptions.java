package com.marklogic.mapreduce;

import javax.net.ssl.SSLContext;

import org.apache.hadoop.io.Writable;

/**
 * Interface used to get options for SSL connection.
 * 
 * @author jchen
 */
public abstract interface SslConfigOptions extends Writable {
    
	public SSLContext getSslContext();
	
	public String[] getEnabledProtocols();
	
	public String[] getEnabledCipherSuites();
}

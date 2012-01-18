/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import javax.net.ssl.SSLContext;

/**
 * Interface used to get options for SSL connection.
 * 
 * <p>
 *   You must implement this interface to use a secure connection to
 *   your input or output MarkLogic Server instance, and then supply
 *   the class as the value of the configuration property
 *   {@link MarkLogicConstants#INPUT_SSL_OPTIONS_CLASS input.usessloptionsclass}
 *   or {@link MarkLogicConstants#OUTPUT_SSL_OPTIONS_CLASS output.usessloptionsclass}.
 * </p>
 * <p>
 *   For details, see the <em>Hadoop MapReduce Connector Developer's Guide</em>.
 *   For an example, see {@link com.marklogic.mapreduce.examples.ContentReader}.
 * </p>
 * 
 * @see MarkLogicConstants
 * @see com.marklogic.mapreduce.examples.ContentReader
 * 
 * @author jchen
 */
public abstract interface SslConfigOptions {
    
    public SSLContext getSslContext();
    
    public String[] getEnabledProtocols();
    
    public String[] getEnabledCipherSuites();
}

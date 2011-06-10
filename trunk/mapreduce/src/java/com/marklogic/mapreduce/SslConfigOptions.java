/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import javax.net.ssl.SSLContext;

/**
 * Interface used to get options for SSL connection.
 * 
 * @author jchen
 */
public abstract interface SslConfigOptions {
    
    public SSLContext getSslContext();
    
    public String[] getEnabledProtocols();
    
    public String[] getEnabledCipherSuites();
}

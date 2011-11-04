/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.VersionInfo;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XSBase64Binary;
import com.marklogic.xcc.types.XSBoolean;
import com.marklogic.xcc.types.XSDouble;
import com.marklogic.xcc.types.XSFloat;
import com.marklogic.xcc.types.XSHexBinary;
import com.marklogic.xcc.types.XSInteger;
import com.marklogic.xcc.types.XdmBinary;

/**
 * Internal utilities shared by the package.  No need to document.
 * 
 * @author jchen
 */
public class InternalUtilities implements MarkLogicConstants {
    public static final Log LOG =
        LogFactory.getLog(MarkLogicConstants.class);
    
    /**
     * Get input server URI based on the job configuration.
     * @param conf job configuration
     * @return server URI
     * @throws URISyntaxException
     */
    static URI getInputServerUri(Configuration conf) throws URISyntaxException {
        String host = conf.get(INPUT_HOST);
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException(INPUT_HOST + 
                    " is not specified.");
        }
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
        String port = conf.get(INPUT_PORT);
        if (port == null || port.isEmpty()) {
            throw new IllegalArgumentException(INPUT_PORT + 
            " is not specified.");
        }
        boolean useSsl = conf.getBoolean(INPUT_USE_SSL, false);
        return getServerUri(user, password, hostName, port, useSsl);
    }
    
    /**
     * Get output server URI based on the job configuration and server host 
     * name.
     * @param conf job configuration
     * @return server URI
     * @throws URISyntaxException
     */
    static URI getOutputServerUri(Configuration conf, String hostName)
    throws URISyntaxException {
        String user = conf.get(OUTPUT_USERNAME, "");
        String password = conf.get(OUTPUT_PASSWORD, "");
        String port = conf.get(OUTPUT_PORT);
        if (port == null) {
            throw new IllegalArgumentException(OUTPUT_PORT + 
            " is not specified.");
        }
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
                    (SslConfigOptions)ReflectionUtils.newInstance(
                            sslOptionClass, conf);
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
                    (SslConfigOptions)ReflectionUtils.newInstance(
                            sslOptionClass, conf);
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
    
    /**
     * Assign value in Writable type from XCC result item.
     * @param <VALUEIN>
     * @param valueClass
     * @param result
     * @param value
     */
    static <VALUEIN> void assignResultValue(Class<? extends Writable> valueClass, 
            ResultItem result, VALUEIN value) {
        if (valueClass.equals(Text.class)) {
            ((Text)value).set(result.asString());
        } else if (valueClass.equals(IntWritable.class) &&
                result.getValueType() == ValueType.XS_INTEGER) {
            XSInteger intItem = (XSInteger)result.getItem();
            ((IntWritable)value).set(intItem.asPrimitiveInt());
        } else if (valueClass.equals(VIntWritable.class) &&
                result.getValueType() == ValueType.XS_INTEGER) {
            XSInteger intItem = (XSInteger)result.getItem();
            ((VIntWritable)value).set(intItem.asPrimitiveInt());
        } else if (valueClass.equals(LongWritable.class) &&
                result.getValueType() == ValueType.XS_INTEGER) {
            XSInteger intItem = (XSInteger)result.getItem();
            ((LongWritable)value).set(intItem.asLong());
        } else if (valueClass.equals(VLongWritable.class) &&
                result.getValueType() == ValueType.XS_INTEGER) {
            XSInteger intItem = (XSInteger)result.getItem();
            ((VLongWritable)value).set(intItem.asLong());
        } else if (valueClass.equals(BooleanWritable.class) &&
                result.getValueType() == ValueType.XS_BOOLEAN) {
            XSBoolean boolItem = (XSBoolean)result.getItem();
            ((BooleanWritable)value).set(boolItem.asPrimitiveBoolean());
        } else if (valueClass.equals(FloatWritable.class) &&
                result.getValueType() == ValueType.XS_FLOAT) {
            XSFloat floatItem = (XSFloat)result.getItem();
            ((FloatWritable)value).set(floatItem.asPrimitiveFloat());
        } else if (valueClass.equals(DoubleWritable.class) &&
                result.getValueType() == ValueType.XS_DOUBLE) {
            XSDouble doubleItem = (XSDouble)result.getItem();
            ((DoubleWritable)value).set(doubleItem.asPrimitiveDouble());
        } else if (valueClass.equals(BytesWritable.class) &&
                result.getValueType() == ValueType.XS_HEX_BINARY) {
            XSHexBinary binItem = (XSHexBinary)result.getItem();
            byte[] bytes = binItem.asBinaryData();
            ((BytesWritable)value).set(bytes, 0, bytes.length);
        } else if (valueClass.equals(BytesWritable.class) &&
                result.getValueType() == ValueType.XS_BASE64_BINARY) {
            XSBase64Binary binItem = (XSBase64Binary)result.getItem();
            byte[] bytes = binItem.asBinaryData();
            ((BytesWritable)value).set(bytes, 0, bytes.length);
        } else if (valueClass.equals(BytesWritable.class) &&
                result.getValueType() == ValueType.BINARY) {
            byte[] bytes = ((XdmBinary)result.getItem()).asBinaryData();
            ((BytesWritable)value).set(bytes, 0, bytes.length);
        } else if (valueClass.equals(MarkLogicNode.class) &&
                (result.getValueType() == ValueType.NODE ||
                 result.getValueType() == ValueType.ELEMENT ||
                 result.getValueType() == ValueType.DOCUMENT ||
                 result.getValueType() == ValueType.ATTRIBUTE)) {
            ((MarkLogicNode)value).set(result);
        } else {
            throw new UnsupportedOperationException("Value " +  
                    valueClass + " is unsupported for result type: " + 
                    result.getValueType());
        }
    }

    public static ContentSource getOutputContentSource(Configuration conf,
            String hostName) 
    throws URISyntaxException, XccConfigException, IOException {
        URI serverUri = getOutputServerUri(conf, hostName);
        return getOutputContentSource(conf, serverUri);
    }

    /**
     * Return the host from the forestHostMap based on the task id in a round
     * robin fashion.
     * 
     * @param taskId
     * @param forestHostMap
     * @return host name
     */
    public static String getHost(int taskId,
            LinkedMapWritable forestHostMap) {
        int count = forestHostMap.size();
        int position = taskId % count;
        int i = 0;
        for (Writable hostName : forestHostMap.values()) {
            if (i++ == position) {
                return hostName.toString();
            }
        }
        throw new IllegalStateException("No host found while taskId = " + 
                taskId + ", forestHostMap.size() = " + count);
    }
    
    /**
     * Check against unsupported versions.
     */
    public static void checkVersion() {
        // check version
        String version = VersionInfo.getVersion();
        if (LOG.isDebugEnabled()) {
            LOG.debug("version: " + VersionInfo.getVersion());
        }
        if (version.startsWith("0.20.203") || 
            version.startsWith("0.20.204")) {
            throw new UnsupportedOperationException(
                    "Hadoop version " + version + " is not supported.");
        }
    }
}
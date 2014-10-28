/*
 * Copyright 2003-2014 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.marklogic.mapreduce.utilities;

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

import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.LinkedMapWritable;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.DatabaseDocument;
import com.marklogic.mapreduce.MarkLogicNode;
import com.marklogic.mapreduce.SslConfigOptions;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.ValueFactory;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XSBase64Binary;
import com.marklogic.xcc.types.XSBoolean;
import com.marklogic.xcc.types.XSDouble;
import com.marklogic.xcc.types.XSFloat;
import com.marklogic.xcc.types.XSHexBinary;
import com.marklogic.xcc.types.XSInteger;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmValue;

/**
 * Internal utilities shared by mapreduce package.  No need to document.
 * 
 * @author jchen
 */
public class InternalUtilities implements MarkLogicConstants {
    public static final Log LOG =
        LogFactory.getLog(InternalUtilities.class);
    
    static final String FOREST_HOST_MAP_QUERY =
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "hadoop:get-forest-host-map()";
    
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
    public static ContentSource getInputContentSource(Configuration conf) 
    throws URISyntaxException, XccConfigException, IOException {
        String host = conf.get(INPUT_HOST);
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException(INPUT_HOST + 
                    " is not specified.");
        }
        
        return getInputContentSource(conf, host);
    }
    
    /**
     * Get input content source.
     *
     * @param conf job configuration
     * @param host host to connect to
     * @return content source
     * @throws IOException 
     * @throws XccConfigException
     */
    public static ContentSource getInputContentSource(Configuration conf,
            String host) 
    throws XccConfigException, IOException {      
        String user = conf.get(INPUT_USERNAME, "");
        String password = conf.get(INPUT_PASSWORD, "");
        String port = conf.get(INPUT_PORT,"8000");
        String db = conf.get(INPUT_DATABASE_NAME);
        int portInt = Integer.parseInt(port);
        boolean useSsl = conf.getBoolean(INPUT_USE_SSL, false);
        if (useSsl) {
            Class<? extends SslConfigOptions> sslOptionClass = 
                conf.getClass(INPUT_SSL_OPTIONS_CLASS, 
                null, SslConfigOptions.class);
            if (sslOptionClass != null) {
                SslConfigOptions sslOptions = 
                    (SslConfigOptions)ReflectionUtils.newInstance(
                            sslOptionClass, conf);
                
                // construct content source
                return getSecureContentSource(host, portInt, user, password, 
                        db, sslOptions);
            }
        }
        return ContentSourceFactory.newContentSource(host, portInt, 
                user, password, db);
    }
    
    static ContentSource getSecureContentSource(String host, int port,
            String user, String password, String db, 
            SslConfigOptions sslOptions) 
    throws XccConfigException {
        ContentSource contentSource = null;
      
        // construct XCC SecurityOptions
        SecurityOptions options = 
            new SecurityOptions(sslOptions.getSslContext());
        options.setEnabledProtocols(sslOptions.getEnabledCipherSuites());
        options.setEnabledProtocols(sslOptions.getEnabledProtocols());
  
        // construct content source
        contentSource = ContentSourceFactory.newContentSource(
                host, port, user, password, db, options);        
 
        return contentSource;
    }
    
    /**
     * Assign value in Writable type from XCC result item.
     * @param <VALUEIN>
     * @param valueClass
     * @param result
     * @param value
     */
    public static <VALUEIN> void assignResultValue(Class<? extends Writable> valueClass, 
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
                 result.getValueType() == ValueType.ATTRIBUTE ||
                 result.getValueType() == ValueType.TEXT)) {
            ((MarkLogicNode)value).set(result);
        } else if (valueClass.equals(DatabaseDocument.class)) {
            ((DatabaseDocument) value).set(result);
        } else {
            throw new UnsupportedOperationException("Value " +  
                    valueClass + " is unsupported for result type: " + 
                    result.getValueType());
        }
    }

    /**
     * Get output content source.
     *
     * @param conf job configuration
     * @param hostName host name
     * @return content source
     * @throws IOException 
     * @throws XccConfigException 
     * @throws IOException 
     */
    public static ContentSource getOutputContentSource(Configuration conf,
            String hostName) 
    throws XccConfigException, IOException {
        String user = conf.get(OUTPUT_USERNAME, "");
        String password = conf.get(OUTPUT_PASSWORD, "");
        String port = conf.get(OUTPUT_PORT,"8000");
        String db = conf.get(OUTPUT_DATABASE_NAME);
        int portInt = Integer.parseInt(port);
        boolean useSsl = conf.getBoolean(OUTPUT_USE_SSL, false);
        if (useSsl) {
            Class<? extends SslConfigOptions> sslOptionClass = 
                conf.getClass(OUTPUT_SSL_OPTIONS_CLASS, 
                null, SslConfigOptions.class);
            if (sslOptionClass != null) {
                SslConfigOptions sslOptions = 
                    (SslConfigOptions)ReflectionUtils.newInstance(
                            sslOptionClass, conf);
                
                // construct content source
                return getSecureContentSource(hostName, portInt, user, password,
                        db, sslOptions);
            }
        }
        return ContentSourceFactory.newContentSource(hostName, portInt, 
                user, password, db);
    }

    /**
     * Return the host from the forestHostMap based on the task id in a round
     * robin fashion.
     * 
     * @param taskId
     * @param forestStatusMap
     * @return host name
     */
    public static String getHost(int taskId,
            LinkedMapWritable forestStatusMap) {
        int count = forestStatusMap.size();
        int position = taskId % count;
        int i = 0;
        for (Writable v : forestStatusMap.values()) {
            ForestInfo fs = (ForestInfo)v;
            if (i++ == position) {
                return fs.getHostName().toString();
            }
        }
        throw new IllegalStateException("No host found while taskId = " + 
                taskId + ", forestHostMap.size() = " + count);
    }
    
    /**
     * Return the host from the host array based on a random fashion
     * @param hosts a WritableArray of host names
     * @return the host name
     */
    public static String getHost(TextArrayWritable hosts) {
        String [] hostStrings = hosts.toStrings();
        int count = hostStrings.length;
        int position = (int)(Math.random() * count);
        return hostStrings[position];
    }
    
    /**
     * Create new XdmValue from value type and Writables.
     *  
     */
    public static XdmValue newValue(ValueType valueType, Object value) {
        if (value instanceof Text) {
            return ValueFactory.newValue(valueType, ((Text)value).toString());
        } else if (value instanceof BytesWritable) {
            return ValueFactory.newValue(valueType, ((BytesWritable)value).getBytes());
        } else if (value instanceof IntWritable) {
            return ValueFactory.newValue(valueType, ((IntWritable)value).get());
        } else if (value instanceof LongWritable) {
            return ValueFactory.newValue(valueType, ((LongWritable)value).get());
        } else if (value instanceof VIntWritable) {
            return ValueFactory.newValue(valueType, ((VIntWritable)value).get());
        } else if (value instanceof VLongWritable) {
            return ValueFactory.newValue(valueType, ((VLongWritable)value).get());
        } else if (value instanceof BooleanWritable) {
            return ValueFactory.newValue(valueType, ((BooleanWritable)value).get());
        } else if (value instanceof FloatWritable) {
            return ValueFactory.newValue(valueType, ((FloatWritable)value).get());
        } else if (value instanceof DoubleWritable) {
            return ValueFactory.newValue(valueType, ((DoubleWritable)value).get());
        } else if (value instanceof MarkLogicNode) {
            return ValueFactory.newValue(valueType, ((MarkLogicNode)value).get());
        } else {
            throw new UnsupportedOperationException("Value " +  
                    value.getClass().getName() + " is unsupported.");
        }
    }
    
    public static String unparse(String s) {
        int len = s.length();
        StringBuilder buf = new StringBuilder(len * 2);
        for(int cp, i = 0; i < s.length(); i += Character.charCount(cp)) {
            cp = s.codePointAt(i);
            // iterate through the codepoints in the string
            if ((cp >= 0x20) && (cp < 0x80)) {
                switch (cp) {
                    case '"':
                        buf.append("&quot;");
                        break;
                    case '&':
                        buf.append("&amp;");
                        break;
                    default:
                        buf.append(s.charAt(i));
                }
            } else {
                buf.append("&#x");
                buf.append(Long.toString(cp, 16));
                buf.append(';');    
            }
        }
        return buf.toString();
    }
    
    /**
     * if outputDir is available and valid, modify DocumentURI, and return uri
     * in string
     * 
     * @param key
     * @param outputDir
     * @return URI
     */
    public static String getUriWithOutputDir(DocumentURI key, String outputDir){
        String uri = key.getUri();
        if (outputDir != null && !outputDir.isEmpty()) {
            uri = outputDir.endsWith("/") || uri.startsWith("/") ? 
                  outputDir + uri : outputDir + '/' + uri;
            key.setUri(uri);
            key.validate();
        }    
        return uri;
    }
    
    public static int compareUnsignedLong(long x, long y) {
        return (x == y) ? 0 : ((x < y) ^ ((x < 0) != (y < 0)) ? -1 : 1);
    }
}
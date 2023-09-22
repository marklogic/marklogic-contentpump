/*
 * Copyright (c) 2023 MarkLogic Corporation
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
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.KeyStoreException;
import java.security.UnrecoverableKeyException;
import java.security.KeyStore;


import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileInputStream;

import com.marklogic.xcc.impl.Credentials;
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

import com.marklogic.contentpump.ConfigConstants;
import com.marklogic.mapreduce.DocumentURI;
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

    private static SslConfigOptions inputSslOptions;
    private static SslConfigOptions outputSslOptions;

    private static Object sslOptionsMutex = new Object();
    
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
        String host = conf.getStrings(INPUT_HOST)[0];
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
        Credentials credentials = getInputCredentials(conf);
        String port = conf.get(INPUT_PORT, ConfigConstants.DEFAULT_PORT);
        String db = conf.get(INPUT_DATABASE_NAME);
        String basePath = conf.get(INPUT_BASE_PATH);
        int portInt = Integer.parseInt(port);
        boolean useSsl = conf.getBoolean(INPUT_USE_SSL, false);
        if (useSsl) {
            return getSecureContentSource(host, portInt, credentials, db,
                basePath, getInputSslOptions(conf));
        }
        return ContentSourceFactory.newContentSource(host, portInt, credentials,
            db, basePath);
    }
    
    private static SslConfigOptions getInputSslOptions(Configuration conf) throws XccConfigException {
        if (null != inputSslOptions) {
            return inputSslOptions;
        }
        synchronized (sslOptionsMutex) {
            if (null != inputSslOptions) {
                return inputSslOptions;
            }
            Class<? extends SslConfigOptions> sslOptionClass = 
                    conf.getClass(INPUT_SSL_OPTIONS_CLASS, 
                    null, SslConfigOptions.class);
            if (sslOptionClass != null) {
                inputSslOptions = 
                        (SslConfigOptions)ReflectionUtils.newInstance(
                                sslOptionClass, conf);
            } else {
                String ssl_protocol = conf.get(INPUT_SSL_PROTOCOL, "TLSv1.2");
                String keystore_path = conf.get(INPUT_KEYSTORE_PATH, null);
                String keystore_passwd = conf.get(INPUT_KEYSTORE_PASSWD, null);
                String truststore_path = conf.get(INPUT_TRUSTSTORE_PATH, null);
                String truststore_passwd = conf.get(INPUT_TRUSTSTORE_PASSWD, null);
                if ((keystore_path == null) && (truststore_path == null) ) {
                    inputSslOptions = new TrustAnyoneOptions(ssl_protocol);
                } else {
                    KeyManager[] keyManager = null;
                    TrustManager[] trustManager = null;
                    if (keystore_path != null) {
                        if (keystore_passwd != null) {
                            try {
                                keyManager = getUserKeyManager(keystore_path, keystore_passwd);
                            } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException | UnrecoverableKeyException e) {
                                throw new XccConfigException("Error constructing SecurityOptions",e);
                            }
                        } else {
                            throw new IllegalArgumentException(INPUT_KEYSTORE_PASSWD +
                                    " is not specified.");
                        }
                    }
                    if (truststore_path != null) {
                        if (truststore_passwd != null) {
                            try {
                                trustManager = getUserTrustManager(truststore_path,truststore_passwd);
                            } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
                                throw new XccConfigException("Error constructing SecurityOptions", e);
                            }
                        }else {
                            throw new IllegalArgumentException(INPUT_TRUSTSTORE_PASSWD +
                                    " is not specified.");
                        }
                    } else {
                        trustManager = getTrustAnyoneManager();
                    }
                    inputSslOptions = new UserTrustOptions(ssl_protocol, keyManager, trustManager);
                }
            }
            return inputSslOptions;
        }
    }
    
    private static SslConfigOptions getOutputSslOptions(Configuration conf) throws XccConfigException {
        if (null != outputSslOptions) {
            return outputSslOptions;
        }
        synchronized (sslOptionsMutex) {
            if (null != outputSslOptions) {
                return outputSslOptions;
            }
            Class<? extends SslConfigOptions> sslOptionClass = 
                    conf.getClass(OUTPUT_SSL_OPTIONS_CLASS, 
                    null, SslConfigOptions.class);
            if (sslOptionClass != null) {
                outputSslOptions = 
                        (SslConfigOptions)ReflectionUtils.newInstance(
                                sslOptionClass, conf);
            } else {
                String ssl_protocol = conf.get(OUTPUT_SSL_PROTOCOL, "TLSv1.2");
                String keystore_path = conf.get(OUTPUT_KEYSTORE_PATH, null);
                String keystore_passwd = conf.get(OUTPUT_KEYSTORE_PASSWD, null);
                String truststore_path = conf.get(OUTPUT_TRUSTSTORE_PATH, null);
                String truststore_passwd = conf.get(OUTPUT_TRUSTSTORE_PASSWD, null);
                if ((keystore_path == null) && (truststore_path == null) ) {
                    outputSslOptions = new TrustAnyoneOptions(ssl_protocol);
                } else {
                    KeyManager[] keyManager = null;
                    TrustManager[] trustManager = null;
                    if (keystore_path != null) {
                        if (keystore_passwd != null) {
                            try {
                                keyManager = getUserKeyManager(keystore_path, keystore_passwd);
                            } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException | UnrecoverableKeyException e) {
                                throw new XccConfigException("Error constructing SecurityOptions",e);
                            }
                        } else {
                            throw new IllegalArgumentException(OUTPUT_KEYSTORE_PASSWD +
                                    " is not specified.");
                        }
                    }
                    if (truststore_path != null) {
                        if (truststore_passwd != null) {
                            try {
                                trustManager = getUserTrustManager(truststore_path,truststore_passwd);
                            } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
                                throw new XccConfigException("Error constructing SecurityOptions", e);
                            }
                        }else {
                            throw new IllegalArgumentException(OUTPUT_TRUSTSTORE_PASSWD +
                                    " is not specified.");
                        }
                    } else {
                        trustManager = getTrustAnyoneManager();
                    }
                    outputSslOptions = new UserTrustOptions(ssl_protocol, keyManager, trustManager);
                }
            }
            return outputSslOptions;
        }
    }

    static class UserTrustOptions implements SslConfigOptions {
        String sslprotocol;
        KeyManager[] keymanager;
        TrustManager[] trustmanager;

        public UserTrustOptions(String sslprotocol, KeyManager[] keymanager, TrustManager[] trustmanager) {
            this.sslprotocol = sslprotocol;
            this.keymanager = keymanager;
            this.trustmanager = trustmanager;
        }
        @Override
        public SSLContext getSslContext() throws NoSuchAlgorithmException,
                KeyManagementException {
            SSLContext sslContext = SSLContext.getInstance(sslprotocol);
            sslContext.init(keymanager, trustmanager, null);

            return sslContext;
        }
        @Override
        public String[] getEnabledProtocols() {
            return null;
        }

        @Override
        public String[] getEnabledCipherSuites() {
            return null;
        }
    }

    static class TrustAnyoneOptions implements SslConfigOptions {
        String sslprotocol;
        public TrustAnyoneOptions(String sslprotocol) {
            this.sslprotocol = sslprotocol;
        }
        @Override
        public SSLContext getSslContext() throws NoSuchAlgorithmException,
                KeyManagementException {
            SSLContext sslContext = SSLContext.getInstance(sslprotocol);
            sslContext.init(null, getTrustAnyoneManager(), null);

            return sslContext;
        }

        @Override
        public String[] getEnabledProtocols() {
            return null;
        }

        @Override
        public String[] getEnabledCipherSuites() {
            return null;
        }
    }

    private static KeyManager[] getUserKeyManager(String path, String password) throws KeyStoreException, IOException,
            NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
        KeyStore ks = KeyStore.getInstance("JKS");
        FileInputStream ksFile = new FileInputStream(path);
        ks.load(ksFile, password.toCharArray());
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, password.toCharArray());
        ksFile.close();
        return kmf.getKeyManagers();
    }

    private static TrustManager[] getUserTrustManager(String path, String password) throws KeyStoreException,
            IOException, NoSuchAlgorithmException, CertificateException {
        KeyStore ks = KeyStore.getInstance("JKS");
        FileInputStream ksFile = new FileInputStream(path);
        ks.load(ksFile, password.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(ks);
        ksFile.close();
        return tmf.getTrustManagers();
    }

    private static TrustManager[] getTrustAnyoneManager() {
        return new TrustManager[] { new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
            /**
             * @throws CertificateException
             */
            public void checkClientTrusted(
                    java.security.cert.X509Certificate[] certs,
                    String authType) throws CertificateException {
                // no exception means it's okay
            }
            /**
             * @throws CertificateException
             */
            public void checkServerTrusted(
                    java.security.cert.X509Certificate[] certs,
                    String authType) throws CertificateException {
                // no exception means it's okay
            }
        } };
    }

    static ContentSource getSecureContentSource(String host, int port,
           Credentials credentials, String db, String basePath,
           SslConfigOptions sslOptions) throws XccConfigException {
        // construct XCC SecurityOptions
        SecurityOptions options;
        try {
            options = new SecurityOptions(sslOptions.getSslContext());
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            throw new XccConfigException("Error constructing SecurityOptions",
                    e);
        }
        options.setEnabledCipherSuites(sslOptions.getEnabledCipherSuites());
        options.setEnabledProtocols(sslOptions.getEnabledProtocols());
  
        // construct content source
        return ContentSourceFactory.newContentSource(
                host, port, credentials, db, basePath, options);
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
        Credentials credentials = getOutputCredentials(conf);
        String port = conf.get(OUTPUT_PORT, ConfigConstants.DEFAULT_PORT);
        String db = conf.get(OUTPUT_DATABASE_NAME);
        String basePath = conf.get(OUTPUT_BASE_PATH);
        int portInt = Integer.parseInt(port);
        boolean useSsl = conf.getBoolean(OUTPUT_USE_SSL, false);
        if (useSsl) {
            return getSecureContentSource(hostName, portInt, credentials, db,
                basePath, getOutputSslOptions(conf));
        }
        return ContentSourceFactory.newContentSource(hostName, portInt,
            credentials, db, basePath);
    }
    
    /**
     * Return the host from the host array based on a random fashion
     * @param hosts a WritableArray of host names
     * @return the host name
     * @throws IOException 
     */
    public static String getHost(TextArrayWritable hosts) throws IOException {
        String [] hostStrings = hosts.toStrings();
        if(hostStrings == null || hostStrings.length==0) 
            throw new IOException("Number of forests is 0: "
                + "check forests in database");
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
     * If outputDir is available and valid, modify DocumentURI, and return uri
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
    
    public static void checkQueryLanguage(String s) {
        if (!(s.equalsIgnoreCase("xquery") || s.equalsIgnoreCase("javascript"))) {
            throw new IllegalArgumentException("Invalid output query language:" + s);
        }
    }

    public static void verifyHosts(String hostList, String portStr) {
        String[] hosts = hostList.split(",");
        int port = Integer.parseInt(portStr);
        for (String host : hosts) {
            InetSocketAddress address = new InetSocketAddress(host, port);
            if (address.isUnresolved()) {
                throw new IllegalArgumentException("host " + host + " is not resolvable");
            }
        }
    }
    
    /**
     * Wake up every 1 second to check whether to abort
     * @param millis
     * @throws InterruptedException 
     */
    public static void sleep(long millis) throws InterruptedException {
        while (millis > 0) {
            // abort if the user kills mlcp in local mode
            String shutdown = System.getProperty("mlcp.shutdown");
            if (shutdown != null) {
                break;
            }
            if (millis > 1000) {
                Thread.sleep(1000);
                millis -= 1000;
            } else {
                Thread.sleep(millis);
                millis = 0;
            }
        }
    }

    private static Credentials getOutputCredentials(Configuration conf) {
        String user = conf.get(OUTPUT_USERNAME, "");
        String password = conf.get(OUTPUT_PASSWORD, "");
        String apiKey = conf.get(OUTPUT_API_KEY);
        String tokenEndpoint = conf.get(OUTPUT_TOKEN_ENDPOINT);
        String grantType = conf.get(OUTPUT_GRANT_TYPE);
        String tokenDuration = conf.get(OUTPUT_TOKEN_DURATION, "0");
        if (apiKey != null) {
            return new Credentials(apiKey.toCharArray(), tokenEndpoint,
                grantType, Integer.parseInt(tokenDuration));
        } else {
            return new Credentials(user, password.toCharArray());
        }
    }

    private static Credentials getInputCredentials(Configuration conf) {
        String user = conf.get(INPUT_USERNAME, "");
        String password = conf.get(INPUT_PASSWORD, "");
        String apiKey = conf.get(INPUT_API_KEY);
        String tokenEndpoint = conf.get(INPUT_TOKEN_ENDPOINT);
        String grantType = conf.get(INPUT_GRANT_TYPE);
        String tokenDuration = conf.get(OUTPUT_TOKEN_DURATION, "0");
        if (apiKey != null) {
            return new Credentials(apiKey.toCharArray(), tokenEndpoint,
                grantType, Integer.parseInt(tokenDuration));
        } else {
            return new Credentials(user, password.toCharArray());
        }
    }
}

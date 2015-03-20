/*
 * Copyright 2003-2015 MarkLogic Corporation
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
package com.marklogic.mapreduce.examples;

import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentInputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.DatabaseDocument;
import com.marklogic.mapreduce.SslConfigOptions;

/**
 * Read documents from MarkLogic Server using an SSL-enabled connection and 
 * write them out to HDFS.  Used with config file 
 * conf/marklogic-docin-textout.xml.
 */
public class ContentReader {
    public static class DocMapper 
    extends Mapper<DocumentURI, DatabaseDocument, DocumentURI, DatabaseDocument> {
        public static final Log LOG =
            LogFactory.getLog(DocMapper.class);
        public void map(DocumentURI key, DatabaseDocument value, Context context) 
        throws IOException, InterruptedException {
            if (key != null && value != null) {
                context.write(key, value);
            } else {
                LOG.error("key: " + key + ", value: " + value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: ContentReader configFile outputDir");
            System.exit(2);
        }
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        Job job = new Job(conf);
        job.setJarByClass(ContentReader.class);
        job.setInputFormatClass(DocumentInputFormat.class);
        job.setMapperClass(DocMapper.class);
        job.setMapOutputKeyClass(DocumentURI.class);
        job.setMapOutputValueClass(DatabaseDocument.class);
        job.setOutputFormatClass(CustomOutputFormat.class);
       
        CustomOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        conf = job.getConfiguration();
        conf.addResource(otherArgs[0]);
        conf.setClass(MarkLogicConstants.INPUT_SSL_OPTIONS_CLASS,  
                SslOptions.class, SslConfigOptions.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    static class SslOptions implements SslConfigOptions {
        @Override
        public String[] getEnabledCipherSuites() {
            return new String[] { "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
                    "TLS_DHE_DSS_WITH_AES_256_CBC_SHA", 
            "TLS_RSA_WITH_AES_256_CBC_SHA" };
        }

        @Override
        public String[] getEnabledProtocols() {
            return new String[] { "TLSv1" };
        }
        
        @Override
        public SSLContext getSslContext() {
            SSLContext sslContext = null;
            try {
                sslContext = SSLContext.getInstance("TLSv1");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            TrustManager[] trustManagers = null;
            // Trust anyone.
            trustManagers = new TrustManager[] { new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
                        throws CertificateException {
                    // nothing to do
                }

                public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
                        throws CertificateException {
                    // nothing to do
                }

                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            } };
           
            KeyManager[] keyManagers = null;
            try {
                sslContext.init(keyManagers, trustManagers, null);
            } catch (KeyManagementException e) {
                e.printStackTrace();
            }
            return sslContext;
        }
    }
    
    static class CustomOutputFormat 
    extends FileOutputFormat<DocumentURI, DatabaseDocument> {
        
        @Override
        public RecordWriter<DocumentURI, DatabaseDocument> getRecordWriter(
                TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new CustomWriter(getOutputPath(context), 
                    context.getConfiguration());
        }
    }

    static class CustomWriter extends RecordWriter<DocumentURI, DatabaseDocument> {

        Path dir;
        Configuration conf;
        FileSystem fs;
        
        public CustomWriter(Path path, Configuration conf) {
            dir = path;
            this.conf = conf;
            try {
                fs = path.getFileSystem(conf);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext arg0) throws IOException,
                InterruptedException {  
        }

        @Override
        public void write(DocumentURI key, DatabaseDocument content)
                throws IOException, InterruptedException {
            Path path = null;
            try {
                URI uri = new URI(key.getUri());
                String pathname = uri.getPath();
                int nameStart = pathname.lastIndexOf('/');
                String filename = nameStart > 0 ? 
                                pathname.substring(pathname.lastIndexOf('/')) :
                                pathname;
                String pathStr = dir.getName() + '/' + filename;
                path = new Path(pathStr);
            
                FSDataOutputStream out = fs.create(path, false);
                System.out.println("writing to: " + path);
                if (content.getContentType() == ContentType.BINARY) {
                    byte[] byteArray = content.getContentAsByteArray();
                    out.write(byteArray, 0, byteArray.length);
                    out.flush();
                    out.close();
                } else {
                    Text text = content.getContentAsText();
                    out.writeChars(text.toString());
                }
            } catch (Exception ex) {
                System.err.println("Failed to create or write to file: " +
                                path);
                ex.printStackTrace();
            }         
        }   
    }
}

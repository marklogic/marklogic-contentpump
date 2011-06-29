package com.marklogic.mapreduce.examples;

import java.io.IOException;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.marklogic.mapreduce.DocumentInputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicNode;
import com.marklogic.mapreduce.SslConfigOptions;

/**
 * Read documents from MarkLogic Server using an SSL-enabled connection and 
 * write them out to HDFS.  Used with config file 
 * conf/marklogic-docin-textout.xml.
 */
public class ContentReader {
    public static class DocMapper 
    extends Mapper<DocumentURI, MarkLogicNode, DocumentURI, MarkLogicNode> {
        public static final Log LOG =
            LogFactory.getLog(DocMapper.class);
        public void map(DocumentURI key, MarkLogicNode value, Context context) 
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
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 1) {
            System.err.println("Usage: ContentReader configFile outputDir");
            System.exit(2);
        }

        Job job = new Job(conf);
        job.setJarByClass(ContentReader.class);
        job.setInputFormatClass(DocumentInputFormat.class);
        job.setMapperClass(DocMapper.class);
        job.setMapOutputKeyClass(DocumentURI.class);
        job.setMapOutputValueClass(MarkLogicNode.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(DocumentURI.class);
        job.setOutputValueClass(MarkLogicNode.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        conf = job.getConfiguration();
        conf.addResource(otherArgs[0]);
        conf.setClass("mapreduce.marklogic.input.ssloptionsclass",  
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
}

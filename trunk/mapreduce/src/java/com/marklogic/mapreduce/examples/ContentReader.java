package com.marklogic.mapreduce.examples;

import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
 * conf/marklogic-docin-docout.xml.
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
		SslOptions options = new SslOptions();
		options.setAcceptAny(true);
		options.setEnabledCipherSuites(
				new String[] { "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
                "TLS_DHE_DSS_WITH_AES_256_CBC_SHA", 
                "TLS_RSA_WITH_AES_256_CBC_SHA" });
		options.setEnabledProtocols(new String[] { "TLSv1" });
		conf.set("mapreduce.marklogic.input.ssloptions", options.toString());
		conf.setClass("mapreduce.marklogic.input.ssloptionsclass",  
				SslOptions.class, SslConfigOptions.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	static class SslOptions implements SslConfigOptions {

		boolean acceptAny; // whether to accept any server certificate
		String[] cipherSuites; // enabled cipher suites
		String[] protocols; // enabled protocols
		
		public void setAcceptAny(boolean acceptAny) {
        	this.acceptAny = acceptAny;
        }
		
		@Override
        public String[] getEnabledCipherSuites() {
	        return cipherSuites;
        }
		
		public void setEnabledCipherSuites(String[] cipherSuites) {
        	this.cipherSuites = cipherSuites;
        }

		@Override
        public String[] getEnabledProtocols() {
	        return null;
        }
		
		public void setEnabledProtocols(String[] protocols) {
			this.protocols = protocols;
		}
		
		@Override
        public SSLContext getSslContext() {
			SSLContext sslContext = null;
			try {
	            sslContext = SSLContext.getInstance(protocols[0]);
            } catch (NoSuchAlgorithmException e) {
	            e.printStackTrace();
            }
            TrustManager[] trustManagers = null;
	        if (acceptAny) {
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
	        }
	        KeyManager[] keyManagers = null;
	        try {
	            sslContext.init(keyManagers, trustManagers, null);
            } catch (KeyManagementException e) {
	            e.printStackTrace();
            }
	        return sslContext;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
	        acceptAny = input.readBoolean(); 
	        ArrayWritable arrayWritable = new ArrayWritable(Text.class);
	        arrayWritable.readFields(input);
	        cipherSuites = convert(arrayWritable);
	        arrayWritable.readFields(input);
	        protocols = convert(arrayWritable);
        }

		private String[] convert(ArrayWritable arrayWritable) {
	        Writable[] fromValues = arrayWritable.get();
	        String[] toValues = new String[fromValues.length];
	        for (int i = 0; i < fromValues.length; i++) {
	        	toValues[i] = ((Text)fromValues[i]).toString();
	        }
	        return toValues;
        }

		@Override
        public void write(DataOutput output) throws IOException {
	        output.writeBoolean(acceptAny);
	        if (cipherSuites == null) {
	        	cipherSuites = new String[] {""};
	        }
	        
	        ArrayWritable arrayWritable = convert(cipherSuites);
            arrayWritable.write(output);
	        if (protocols == null) {
	            protocols = new String[] {""};
	        } 
	        arrayWritable = convert(protocols);
            arrayWritable.write(output);
        }
		
		private ArrayWritable convert(String[] strArray) {
			Text[] textArray = new Text[strArray.length];
			for (int i = 0; i < strArray.length; i++) {
				textArray[i] = new Text(strArray[i]);
			}
			return new ArrayWritable(Text.class, textArray);
		}
		
		public String toString() {
			DefaultStringifier<SslOptions> stringifier = 
				new DefaultStringifier<SslOptions>(new Configuration(),
					SslOptions.class);
			try {
	            return stringifier.toString(this);
            } catch (IOException e) {
	            e.printStackTrace();
	            return "";
            }
		}
		
	}
}

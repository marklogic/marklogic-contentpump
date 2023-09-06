/*
 * Copyright (c) 2020 MarkLogic Corporation

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
package com.marklogic.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import com.marklogic.contentpump.LocalJob;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.mapreduce.utilities.TextArrayWritable;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.ServerConnectionException;

/**
 * MarkLogic-based OutputFormat superclass. Use the provided subclasses, such
 * as {@link PropertyOutputFormat} to configure your job.
 * 
 * @author jchen
 */
public abstract class MarkLogicOutputFormat<KEYOUT, VALUEOUT> 
extends OutputFormat<KEYOUT, VALUEOUT> 
implements MarkLogicConstants, Configurable {
    public static final Log LOG =
        LogFactory.getLog(MarkLogicOutputFormat.class);
    
    static final String DIRECTORY_TEMPLATE = "{dir}";
    static final String DELETE_DIRECTORY_TEMPLATE = 
        "xdmp:directory-delete(\"" + DIRECTORY_TEMPLATE + "\")";
    static final String CHECK_DIRECTORY_EXIST_TEMPLATE = 
        "exists(xdmp:directory(\"" + DIRECTORY_TEMPLATE + 
        "\", \"infinity\"))";

    static final String DIRECTORY_CREATE_QUERY = 
        "import module namespace hadoop = " + 
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "hadoop:get-directory-creation()";

    public static final String HOSTS_QUERY = "import module namespace hadoop = "
        + "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"
        + "let $f := "
        + "  fn:function-lookup(xs:QName('hadoop:get-host-names'),0)\n"
        + "let $f2 := "
        + "  fn:function-lookup(xs:QName('hadoop:get-replica-host-names'),0)\n"
        + "return  if (exists($f2)) then $f2() else \n"
        + "   if(exists($f)) then $f() else\n"
        + "   for $i at $p in hadoop:get-forest-host-map()"
        + "   where $p mod 2 eq 0 "
        + "   return $i";
    static final String MANUAL_DIRECTORY_MODE = "manual";
    protected Configuration conf;

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException {
        String[] hosts = conf.getStrings(OUTPUT_HOST);
        if (hosts == null || hosts.length == 0) {
            throw new IllegalStateException(OUTPUT_HOST +
                    " is not specified.");
        }
        for (String host : hosts) {
            try {
                ContentSource cs = InternalUtilities.getOutputContentSource(conf,
                    host);
                ((LocalJob) context).getThreadManager().queryServerMaxThreads(cs);
                checkOutputSpecs(conf, cs, context);
                return;
            } catch (Exception ex) {
                if (ex instanceof ServerConnectionException) {
                    LOG.warn("ServerConnectionException:" + ex.getMessage() +
                        " .Unable to connect to " + host
                        + " to query destination information");
                } else {
                    LOG.warn("Exception:" + ex.getMessage());
                    throw new IOException(ex);
                }
            }
        }
        // No usable output hostname found at this point
        throw new IOException("Unable to query destination information,"
                + " no usable hostname found");
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new OutputCommitter() {
            public void abortTask(TaskAttemptContext taskContext) { }
            public void commitTask(TaskAttemptContext taskContext) { }
            public boolean needsTaskCommit(TaskAttemptContext taskContext) {
              return false;
            }
            public void setupJob(JobContext jobContext) { }
            public void setupTask(TaskAttemptContext taskContext) { }
          };
    }  

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;        
    }
    

    
    protected TextArrayWritable getHosts(Configuration conf) throws IOException {
        String forestHost = conf.get(OUTPUT_FOREST_HOST);
        if (forestHost != null) {
            // Restores the object from the configuration.
            TextArrayWritable hosts = DefaultStringifier.load(conf,
                OUTPUT_FOREST_HOST, TextArrayWritable.class);
            return hosts;
        } else {
            throw new IOException("Forest host map not found");
        }
    }
    
    protected TextArrayWritable queryHosts(ContentSource cs) 
    		throws IOException {
    	return queryHosts(cs, null, null);
    }
    
   
    // Query for a list a hosts, replacing any host name matching hostName 
    // with outputHost
    protected TextArrayWritable queryHosts(ContentSource cs, String matchHost,
    		String replaceHost)
        throws IOException {
        Session session = null;
        ResultSequence result = null;
        try {
            session = cs.newSession();
            AdhocQuery query = session.newAdhocQuery(HOSTS_QUERY);
            // query hosts
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");
            query.setOptions(options);
            result = session.submitRequest(query);

            ArrayList<Text> hosts = new ArrayList<>();
            while (result.hasNext()) {
                ResultItem item = result.next();
                String host = item.asString();
                if (matchHost != null && host.equals(matchHost)) {
                	hosts.add(new Text(replaceHost));
                } else {
                    hosts.add(new Text(host));
                }
            }
            if (hosts.isEmpty()) {
                throw new IOException("Target database has no forests attached: "
                        + "check forests in database");
            }
            return new TextArrayWritable(hosts.toArray(new Text[hosts.size()]));
        } catch (RequestException e) {
            LOG.error(e.getMessage(), e);
            throw new IOException(e);
        } finally {
            if (result != null) {
                result.close();
            }
            if (session != null) {
                session.close();
            }
        }
    }

    public abstract void checkOutputSpecs(
        Configuration conf, ContentSource cs, JobContext context)
    throws IOException;
}

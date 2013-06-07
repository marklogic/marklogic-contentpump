/*
 * Copyright 2003-2013 MarkLogic Corporation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.mapreduce.utilities.AssignmentPolicy;
import com.marklogic.mapreduce.utilities.ForestInfo;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.mapreduce.utilities.TextArrayWritable;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

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
        + "let $hasNewFn := exists($f)\n"
        + "return  if($hasNewFn eq fn:true()) then $f()"
        + " else hadoop:get-forest-host-map()";
    static final String MANUAL_DIRECTORY_MODE = "manual";
    
    protected Configuration conf;

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException,
            InterruptedException {
        String host = conf.get(OUTPUT_HOST);
        if (host == null || host.isEmpty()) {
            throw new IllegalStateException(OUTPUT_HOST +
                    " is not specified.");
        }                     

        try {
            // try getting a connection
            ContentSource cs = InternalUtilities.getOutputContentSource(conf,
                host);
            checkOutputSpecs(conf, cs);
        } 
        catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                context);
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
            try {
                // try getting a connection
                ContentSource cs = InternalUtilities.getOutputContentSource(
                    conf, conf.get(OUTPUT_HOST));
                // query hosts
                return queryHosts(cs);
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }
    
    protected TextArrayWritable queryHosts(ContentSource cs)
        throws IOException {
        if( conf.get(OUTPUT_PARTITION)!= null) {
            LOG.warn("output_partition is ommited in non-fastload mode");
        }
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

            ArrayList<Text> hosts = new ArrayList<Text>();
            while (result.hasNext()) {
                ResultItem item = result.next();
                // skip the forestid in case MLCP 7 runs against ML6
                if(item.asString().matches("-?\\d+"))
                    continue;
                Text host = new Text(item.asString());
                hosts.add(host);
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
    

    
    public abstract void checkOutputSpecs(Configuration conf, ContentSource cs) 
    throws IOException;
}
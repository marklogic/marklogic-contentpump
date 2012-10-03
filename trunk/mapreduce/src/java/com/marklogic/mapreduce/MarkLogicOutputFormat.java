/*
 * Copyright 2003-2012 MarkLogic Corporation
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
    static final String FOREST_HOST_MAP_QUERY =
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "hadoop:get-forest-host-map()";
    static final String DIRECTORY_CREATE_QUERY = 
        "import module namespace hadoop = " + 
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "hadoop:get-directory-creation()";
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
            
            // query forest host mapping            
            LinkedMapWritable forestHostMap = queryForestHostMap(cs);
            
            // store it into config system
            DefaultStringifier.store(conf, forestHostMap, OUTPUT_FOREST_HOST);
            
            checkOutputSpecs(conf, cs);
        } catch (Exception ex) {
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
    
    // forest host map is saved when checkOutputSpecs() is called.  In certain 
    // versions of Hadoop, the config is not persisted as part of the job hence
    // will be lost.  See MAPREDUCE-3377 for details.  When this entry cannot
    // be found from the config, re-query the database to get this info.  It is
    // possible that each task gets a different version of the map if the 
    // forest config changes while the job runs.
    protected LinkedMapWritable getForestHostMap(Configuration conf) 
    throws IOException {
        String forestHost = conf.get(OUTPUT_FOREST_HOST);
        if (forestHost != null) {
            return DefaultStringifier.load(conf, OUTPUT_FOREST_HOST, 
                            LinkedMapWritable.class);
        } else {
            try {                
                // try getting a connection
                ContentSource cs = 
                    InternalUtilities.getOutputContentSource(conf, 
                            conf.get(OUTPUT_HOST));
                
                // query forest host mapping            
                return queryForestHostMap(cs);
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }
    
    protected LinkedMapWritable queryForestHostMap(ContentSource cs) 
    throws IOException {      
        Session session = null;
        ResultSequence result = null;
        try {
            session = cs.newSession();   
            
            // query forest host mapping            
            AdhocQuery query = session.newAdhocQuery(FOREST_HOST_MAP_QUERY);
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");
            query.setOptions(options);
            
            result = session.submitRequest(query);
            LinkedMapWritable forestHostMap = new LinkedMapWritable();
            Text forest = null;
            while (result.hasNext()) {
                ResultItem item = result.next();
                if (forest == null) {
                    forest = new Text(item.asString());            
                } else {
                    Text hostName = new Text(item.asString());
                    forestHostMap.put(forest, hostName);
                    forest = null;
                }
            }
            return forestHostMap;
        } catch (RequestException e) {
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
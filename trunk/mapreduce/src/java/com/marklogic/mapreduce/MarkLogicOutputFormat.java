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
import org.apache.hadoop.io.ArrayWritable;
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
import com.marklogic.mapreduce.utilities.ForestStatus;
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
    static final String FOREST_HOST_MAP_QUERY =
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "hadoop:get-forest-host-map()";
    public static final String FOREST_STATUS_MAP_REBALANCING_QUERY =
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "hadoop:get-forest-status-map-for-rebalancing()";
    static final String DIRECTORY_CREATE_QUERY = 
        "import module namespace hadoop = " + 
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "hadoop:get-directory-creation()";
    public static final String ASSIGNMENT_POLICY_QUERY =
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "hadoop:get-assignment-policy()";
    public static final String HOSTS_QUERY = 
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "hadoop:get-host-names()";
    static final String MANUAL_DIRECTORY_MODE = "manual";
    
    protected Configuration conf;
    protected AssignmentManager am = AssignmentManager.getInstance();
    /**
     * whether server has assignment policy
     */
    protected boolean serverHasPolicy;
    
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
    
    // forest host map is saved when checkOutputSpecs() is called.  In certain 
    // versions of Hadoop, the config is not persisted as part of the job hence
    // will be lost.  See MAPREDUCE-3377 for details.  When this entry cannot
    // be found from the config, re-query the database to get this info.  It is
    // possible that each task gets a different version of the map if the 
    // forest config changes while the job runs.
    protected LinkedMapWritable getForestStatusMap(Configuration conf) 
    throws IOException {
        String forestHost = conf.get(OUTPUT_FOREST_HOST);
        if (forestHost != null) {
            //Restores the object from the configuration.
            LinkedMapWritable fhmap = DefaultStringifier.load(conf, OUTPUT_FOREST_HOST, 
                LinkedMapWritable.class);
            String s = conf.get(ASSIGNMENT_POLICY);
            AssignmentPolicy.Kind kind = null;
            if (s == null) {
                // server prior to 7, no assignment policy, use legacy
                kind = AssignmentPolicy.Kind.LEGACY;
            } else {
                kind = AssignmentPolicy.Kind.valueOf(s.toUpperCase());
            }
            am.initialize(kind, fhmap);
            return fhmap;
        } else {
            try {
                // try getting a connection
                ContentSource cs = 
                    InternalUtilities.getOutputContentSource(conf, 
                            conf.get(OUTPUT_HOST));
                // query forest status mapping
                return queryForestStatusMap(cs);
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }
    
    protected ArrayWritable getHosts(Configuration conf) throws IOException {
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
    
    /**
     * it is called only if the server has policy
     * @param session
     * @return assignment policy
     * @throws IOException
     * @throws RequestException
     */
    protected AssignmentPolicy.Kind getAssignmentPolicy(Session session)
        throws IOException, RequestException {
        AdhocQuery query = session.newAdhocQuery(ASSIGNMENT_POLICY_QUERY);
        RequestOptions options = new RequestOptions();
        options.setDefaultXQueryVersion("1.0-ml");
        query.setOptions(options);
        ResultSequence result = null;
        result = session.submitRequest(query);

        AssignmentPolicy.Kind kind = null;
        ResultItem item = result.next();
        String s = item.asString();
        conf.set(ASSIGNMENT_POLICY, s);
        kind = AssignmentPolicy.Kind.valueOf(s.toUpperCase());
        item = result.next();
        if (kind == AssignmentPolicy.Kind.STATISTICAL
            && Boolean.parseBoolean(item.asString())
            && conf.getBoolean(OUTPUT_FAST_LOAD, false)) {
            throw new IOException(
                "Fastload can't be used:" +
                "rebalancer is on and assignment policy is statistical");
        }
        return kind;
    }
    
    protected boolean hasAssignmentPolicy(Session session)
        throws RequestException {
        AdhocQuery query = session
            .newAdhocQuery("import module namespace hadoop = "
                + "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"
                + "let $f := fn:function-lookup(xs:QName('hadoop:get-assignment-policy'),0)\n"
                + "return exists($f)");
        RequestOptions options = new RequestOptions();
        options.setDefaultXQueryVersion("1.0-ml");
        query.setOptions(options);
        ResultSequence result = null;
        result = session.submitRequest(query);
        String item = result.asString();
        return Boolean.parseBoolean(item);
    }
    
    protected ArrayWritable queryHosts(ContentSource cs)
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

            ArrayList<Text> hosts = new ArrayList<Text>();
            while (result.hasNext()) {
                ResultItem item = result.next();
                Text host = new Text(item.asString());
                hosts.add(host);
            }
            return new ArrayWritable(Text.class, hosts.toArray(new Text[hosts.size()]));
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
    
    protected LinkedMapWritable queryForestStatusMap(ContentSource cs) 
    throws IOException {
        Session session = null;
        ResultSequence result = null;
        try {
            session = cs.newSession();   
            AdhocQuery query = null;
            AssignmentPolicy.Kind kind = null;
            if (!serverHasPolicy) {
                serverHasPolicy = hasAssignmentPolicy(session);
            }
            if (!serverHasPolicy) {
                //server prior to 7
                LOG.warn("No assignment policy in server, use legacy assignment policy.");
                kind = AssignmentPolicy.Kind.LEGACY;
                query = session.newAdhocQuery(FOREST_HOST_MAP_QUERY);
            } else {
                kind = getAssignmentPolicy(session);
                if (kind == AssignmentPolicy.Kind.RANGE) {
                    String pName = conf.get(OUTPUT_PARTITION);
                    if (pName == null) {
                        throw new IOException(
                            "output_partition_name is not set in fastload "
                                + "mode while server uses range assignment policy");
                    } else {
                        query = session
                            .newAdhocQuery(getForestHostMapPartitionQuery(pName));
                    }
                } else {
                    query = session
                        .newAdhocQuery(FOREST_STATUS_MAP_REBALANCING_QUERY);
                }
            }

            // query forest status mapping       
            
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");
            query.setOptions(options);
            if(LOG.isDebugEnabled()) {
                LOG.debug(query);
            }
            result = session.submitRequest(query);

            LinkedMapWritable forestStatusMap = new LinkedMapWritable();
            Text forest = null;
            while (result.hasNext()) {
                ResultItem item = result.next();
                if (forest == null) {
                    forest = new Text(item.asString());
                } else {
                    Text hostName = new Text(item.asString());
                    if (serverHasPolicy) {
                        item = result.next();
                        BooleanWritable updatable = new BooleanWritable(
                            Boolean.parseBoolean(item.asString()));
                        item = result.next();
                        LongWritable dc = new LongWritable(Long.parseLong(item
                            .asString()));
                        forestStatusMap.put(forest, new ForestStatus(hostName,
                            dc, updatable));
                    } else {
                        forestStatusMap.put(forest, new ForestStatus(hostName,
                            new LongWritable(0), new BooleanWritable(true)));
                    }
                    forest = null;
                }
            }
            if (forestStatusMap.size() == 0) {
                if (kind == AssignmentPolicy.Kind.RANGE) {
                    throw new IOException("Number of forests is 0: "
                        + "check server license for tiered storage; "
                        + "check partition_name");
                } else {
                    throw new IOException("Number of forests is 0: "
                        + "check forests in database");
                }
            }
            am.initialize(kind, forestStatusMap);
            return forestStatusMap;
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
    
    private String getForestHostMapPartitionQuery(String pName) {
        String query =
            "import module namespace hadoop = " +
            "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
            "hadoop:get-forest-status-map-for-partition(\"" + pName + "\")";
        return query;
    }
    
    public abstract void checkOutputSpecs(Configuration conf, ContentSource cs) 
    throws IOException;
}
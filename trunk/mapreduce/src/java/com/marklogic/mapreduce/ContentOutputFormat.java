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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.mapreduce.utilities.AssignmentPolicy;
import com.marklogic.mapreduce.utilities.ForestInfo;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.mapreduce.utilities.TextArrayWritable;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.XSBoolean;

/**
 * MarkLogicOutputFormat for Content.
 * 
 * <p>
 *  Use this class to store results as content in a MarkLogic Server database.
 *  The text, XML, or binary content is inserted into the database at the
 *  given {@link DocumentURI}.
 * </p>
 * <p>
 *  When using this {@link MarkLogicOutputFormat}, your key should be the URI of
 *  the document to insert into the database. The value should be the content to
 *  insert, in the form of {@link org.apache.hadoop.io.Text} or 
 *  {@link MarkLogicNode}.
 * </p>
 * <p>
 *  Several configuration properties exist for controlling the content insertion,
 *  including permissions, collections, quality, directory, and content type.
 * </p>
 * 
 * @see MarkLogicConstants
 * @see com.marklogic.mapreduce.examples.ContentLoader
 * @see com.marklogic.mapreduce.examples.ZipContentLoader
 * @author jchen
 *
 * @param <VALUEOUT>
 */
public class ContentOutputFormat<VALUEOUT> extends
        MarkLogicOutputFormat<DocumentURI, VALUEOUT> {
    public static final Log LOG = LogFactory.getLog(ContentOutputFormat.class);
    
    // Prepend to a forest id to form a database name parsed by XDBC.
    // Also used here alone as the forest id placeholder in non-fast-mode.
    public static final String ID_PREFIX = "#";
    
    static final String FOREST_HOST_MAP_QUERY =
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "hadoop:get-forest-host-map()";
    public static final String FOREST_STATUS_MAP_REBALANCING_QUERY =
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "declare variable $policy as xs:string external;\n" +
        "declare variable $partition-name as xs:string external;\n" + 
        "hadoop:get-forest-status-map-for-rebalancing($policy,$partition-name)";
    public static final String ASSIGNMENT_POLICY_QUERY =
        "import module namespace hadoop = "
        + "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"
        + "let $f := "
        + "  fn:function-lookup(xs:QName('hadoop:get-assignment-policy'),0)\n"
        + "let $hasPolicy := exists($f)"
        + "return  if($hasPolicy eq fn:true()) then "
        + "$f()\n" + "else ()";
    
    protected AssignmentManager am = AssignmentManager.getInstance();
    protected boolean fastLoad;
    protected AssignmentPolicy.Kind plcyKind;
    @Override
    public void checkOutputSpecs(Configuration conf, ContentSource cs) 
    throws IOException { 
        Session session = null;
        ResultSequence result = null;
        try {
            session = cs.newSession(); 
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");
            session.setDefaultRequestOptions(options);
            
            // clear output dir if specified
            String outputDir = conf.get(OUTPUT_DIRECTORY);
            if (outputDir != null) {
                outputDir = outputDir.endsWith("/") ? 
                        outputDir : outputDir + "/";
                if (conf.getBoolean(OUTPUT_CLEAN_DIR, false)) {
                    // delete directory if exists
                    String queryText = DELETE_DIRECTORY_TEMPLATE.replace(
                            DIRECTORY_TEMPLATE, outputDir);
                    AdhocQuery query = session.newAdhocQuery(queryText);
                    result = session.submitRequest(query);
                } else { // ensure nothing exists under output dir
                    String queryText = CHECK_DIRECTORY_EXIST_TEMPLATE.replace(
                            DIRECTORY_TEMPLATE, outputDir);
                    AdhocQuery query = session.newAdhocQuery(queryText);
                    result = session.submitRequest(query);
                    if (result.hasNext()) {
                        ResultItem item = result.next();
                        if (((XSBoolean)(item.getItem())).asBoolean()) {
                            throw new IllegalStateException("Directory " + 
                                    outputDir + " already exists");
                        }
                    } else {
                        throw new IllegalStateException(
                                "Failed to query directory content.");
                    }
                }
            }
            fastLoad = isFastLoad(cs);
            // ensure manual directory creation 
            if (fastLoad) {
                LOG.info("Running in fast load mode");
                    // store forest-info map into config system
                DefaultStringifier.store(conf, queryForestInfo(cs),
                    OUTPUT_FOREST_HOST);

                AdhocQuery query = session.newAdhocQuery(
                                DIRECTORY_CREATE_QUERY);
                result = session.submitRequest(query);
                if (result.hasNext()) {
                    ResultItem item = result.next();
                    String dirMode = item.asString();
                    if (!dirMode.equals(MANUAL_DIRECTORY_MODE)) {
                        throw new IllegalStateException(
                                "Manual directory creation mode is required. " +
                                "The current creation mode is " + dirMode + ".");
                    }
                } else {
                    throw new IllegalStateException(
                            "Failed to query directory creation mode.");
                }
            } else {
                TextArrayWritable hostArray = queryHosts(cs);
                // store hosts into config system
                DefaultStringifier.store(conf, hostArray, OUTPUT_FOREST_HOST);
            }
    
            // validate capabilities
            String[] perms = conf.getStrings(OUTPUT_PERMISSION);
            if (perms != null && perms.length > 0) {
                if (perms.length % 2 != 0) {
                    throw new IllegalStateException(
                    "Permissions are expected to be in <role, capability> pairs.");
                }
                int i = 0;
                while (i + 1 < perms.length) {
                    String roleName = perms[i++];
                    if (roleName == null || roleName.isEmpty()) {
                        throw new IllegalStateException(
                                "Illegal role name: " + roleName);
                    }
                    String perm = perms[i].trim();
                    if (!perm.equalsIgnoreCase(ContentCapability.READ.toString()) &&
                        !perm.equalsIgnoreCase(ContentCapability.EXECUTE.toString()) &&
                        !perm.equalsIgnoreCase(ContentCapability.INSERT.toString()) &&
                        !perm.equalsIgnoreCase(ContentCapability.UPDATE.toString())) {
                        throw new IllegalStateException("Illegal capability: " + perm);
                    }
                    i++;
                }
            }
        } catch (RequestException ex) {
            throw new IOException(ex);
        } finally {
            if (session != null) {
                session.close();
            } 
            if (result != null) {
                result.close();
            }
        }
    }
    
    protected Map<String, ContentSource> getSourceMap(boolean fastLoad, TaskAttemptContext context) throws IOException{
        Configuration conf = context.getConfiguration();
        Map<String, ContentSource> sourceMap = 
            new LinkedHashMap<String, ContentSource>();
        if (fastLoad) {
            LinkedMapWritable forestStatusMap = getForestStatusMap(conf);
            // get host->contentSource mapping
            Map<String, ContentSource> hostSourceMap = 
                new HashMap<String, ContentSource>();
            for (Writable v : forestStatusMap.values()) {
                ForestInfo fs = (ForestInfo)v;
                //unupdatable forests
                if(fs.getUpdatable() == false) continue;
                if (hostSourceMap.get(fs.getHostName()) == null) {
                    try {
                        ContentSource cs = InternalUtilities.getOutputContentSource(
                            conf, fs.getHostName().toString());
                        hostSourceMap.put(fs.getHostName(), cs);
                    } catch (XccConfigException e) {
                        throw new IOException(e);
                    } 
                }
            }
            
            // consolidate forest->host map and host-contentSource map to 
            // forest-contentSource map
            for (Writable forestId : forestStatusMap.keySet()) {
                String forest = ((Text)forestId).toString();
                String hostName = ((ForestInfo)forestStatusMap.get(forestId)).getHostName();
                ContentSource cs = hostSourceMap.get(hostName);
                sourceMap.put(ID_PREFIX + forest, cs);
            }
        } else {
            TextArrayWritable hosts = getHosts(conf);
            int taskId = context.getTaskAttemptID().getTaskID().getId();
            String host = InternalUtilities.getHost(taskId, hosts);
            try {
                ContentSource cs = InternalUtilities.getOutputContentSource(
                    conf, host);
                sourceMap.put(ID_PREFIX, cs);
            } catch (XccConfigException e) {
                throw new IOException(e);
            }
        }
        return sourceMap;
    }
    
    @Override
    public RecordWriter<DocumentURI, VALUEOUT> getRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fastLoad = Boolean.valueOf(conf.get(OUTPUT_FAST_LOAD));
        Map<String, ContentSource> sourceMap = getSourceMap(fastLoad, context);
        // construct the ContentWriter
        return new ContentWriter<VALUEOUT>(conf, sourceMap, fastLoad, am);
    }
    
    protected boolean isFastLoad(ContentSource cs) throws IOException {
      //fastLoad default: not set
        boolean fastLoad = false;
        boolean isRange = isRangePolicy(cs);
        if (conf.get(OUTPUT_FAST_LOAD) == null) {
            // fastload not set
            if (conf.get(OUTPUT_DIRECTORY) != null) {
                // output_dir is set, attempt to do fastload
                if(conf.get(OUTPUT_PARTITION)== null && isRange) {
                    LOG.warn("output_partition is not set for range policy, fastload mode is off");
                    fastLoad = false;
                } else {
                    fastLoad = true;
                }
            } else {
                //neither fastload nor output_dir is set
                fastLoad = false;
            }
        } else {
            // if fastload is set, we honor it
            fastLoad = new Boolean(conf.get(OUTPUT_FAST_LOAD));
            if (fastLoad && conf.get(OUTPUT_PARTITION) == null
                && isRange) {
                throw new IOException(
                    "output_partition is not set in fastload "
                        + "mode while server uses range assignment policy");
            }
        }
        conf.setBoolean(OUTPUT_FAST_LOAD, fastLoad);
        return fastLoad;
    }
    
    /**
     * check assignment policy, set plcyKind
     * @param cs
     * @return true if it is range policy
     * @throws IOException
     */
    protected boolean isRangePolicy(ContentSource cs) throws IOException {
        Session session = null;
        ResultSequence result = null;
        try {
            session = cs.newSession();   
            if (am.getPolicy() == null) {
                plcyKind = getAssignmentPolicy(session);
            } else {
                plcyKind = am.getPolicy().getPolicyKind();
            }
            if (plcyKind != null && plcyKind == AssignmentPolicy.Kind.RANGE) {
                return true;
            }
            return false;
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
            // must be in fast load mode, otherwise won't reach here
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
                return queryForestInfo(cs);
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }
    
    /**
     * it is called only if the server has policy
     * 
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
        if (!result.hasNext())
            return null;

        ResultItem item = result.next();
        String s = item.asString();
        conf.set(ASSIGNMENT_POLICY, s);
        AssignmentPolicy.Kind kind = AssignmentPolicy.Kind.valueOf(s
            .toUpperCase());
        item = result.next();
        if ((kind == AssignmentPolicy.Kind.STATISTICAL 
            || kind == AssignmentPolicy.Kind.RANGE)
            && Boolean.parseBoolean(item.asString()) && fastLoad) {
            throw new IOException(
                "Fastload can't be used:"
                    + "rebalancer is on and assignment policy is statistical or range");
        }
        return kind;
    }

    /**
     * must be attempting or doing fastload when this method is called.
     * result format of the query varies based on policy
     * 
     * bucket:(fid, host, updateAllow)*
     * range:(fid, host, fragmentCount)*
     * statistical: (fid, host, fragmentCount)*
     * legacy: (fid, host)*
     * 
     * @param cs
     * @return a forest-info map
     * @throws IOException
     */
    protected LinkedMapWritable queryForestInfo(ContentSource cs) 
    throws IOException {
        Session session = null;
        ResultSequence result = null;
        try {
            session = cs.newSession();   
            AdhocQuery query = null;
            boolean hasPolicy = plcyKind != null;
            if (!hasPolicy) {
                //no assignment policy, server prior to 7
                LOG.warn("No assignment policy in server, use legacy assignment policy.");
                plcyKind = AssignmentPolicy.Kind.LEGACY;
                query = session.newAdhocQuery(FOREST_HOST_MAP_QUERY);
            } else {
                query = session
                .newAdhocQuery(FOREST_STATUS_MAP_REBALANCING_QUERY);
                if (plcyKind == AssignmentPolicy.Kind.RANGE) {
                    String pName = conf.get(OUTPUT_PARTITION);
                    query.setNewStringVariable("partition-name", pName);
                } else {
                    query.setNewStringVariable("partition-name", "");
                }
                query.setNewStringVariable("policy", plcyKind.toString()
                    .toLowerCase());
            }

            // query forest status mapping       
            
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");
            query.setOptions(options);
            if(LOG.isDebugEnabled()) {
                LOG.debug(query.toString());
            }
            result = session.submitRequest(query);

            LinkedMapWritable forestStatusMap = new LinkedMapWritable();
            Text forest = null;
            while (result.hasNext()) {
                ResultItem item = result.next();
                if (forest == null) {
                    forest = new Text(item.asString());
                } else {
                    String hostName = item.asString();
                    if (hasPolicy) {
                        if (plcyKind == AssignmentPolicy.Kind.BUCKET) {
                            item = result.next();
                            boolean updatable = Boolean.parseBoolean(item
                                .asString());
                            forestStatusMap.put(forest, new ForestInfo(
                                hostName, -1, updatable));
                        } else if (plcyKind == AssignmentPolicy.Kind.LEGACY) {
                            forestStatusMap.put(forest, new ForestInfo(
                                hostName, -1, true));
                        } else {
                            // range or statistical
                            item = result.next();
                            long dc = Long.parseLong(item.asString());
                            forestStatusMap.put(forest, new ForestInfo(
                                hostName, dc, true));
                        }
                    } else {
                        forestStatusMap.put(forest, new ForestInfo(hostName,
                            -1, true));
                    }
                    forest = null;
                }
            }
            if (forestStatusMap.size() == 0) {
                throw new IOException("Number of forests is 0: "
                    + "check forests in database");
            }
            am.initialize(plcyKind, forestStatusMap);
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
    
}

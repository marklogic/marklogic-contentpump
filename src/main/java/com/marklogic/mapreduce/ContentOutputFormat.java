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
package com.marklogic.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.marklogic.contentpump.LocalJob;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.http.HttpChannel;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.mapreduce.utilities.AssignmentPolicy;
import com.marklogic.mapreduce.utilities.ForestInfo;
import com.marklogic.mapreduce.utilities.ForestHost;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.mapreduce.utilities.RestrictedHostsUtil;
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
import com.marklogic.xcc.types.ItemType;
import com.marklogic.xcc.types.XSBoolean;
import com.marklogic.xcc.types.XSInteger;

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
    public static final String FOREST_HOST_QUERY =
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "declare variable $policy as xs:string external;\n" +
        "declare variable $partition-name as xs:string external;\n" + 
        "hadoop:get-forest-host($policy,$partition-name)";
    public static final String FOREST_REPLICA_HOST_QUERY =
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "declare variable $policy as xs:string external;\n" +
        "declare variable $partition-name as xs:string external;\n" + 
        "hadoop:get-forest-replica-hosts($policy,$partition-name)";
    // For supporting backward compatibility for segment policy
    public static final String FOREST_REPLICA_HOST_QUERY_WITH_SEGMENT =
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "declare variable $policy as xs:string external;\n" +
        "declare variable $partition-name as xs:string external;\n" +
        "hadoop:get-forest-replica-hosts-with-segment($policy,$partition-name)";
    public static final String INIT_QUERY =
        "import module namespace hadoop = "
      + "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"
      + "xdmp:host-name(xdmp:host()), \n"
      + "let $versionf := "
      + "  fn:function-lookup(xs:QName('xdmp:effective-version'),0)\n"
      + "return if (exists($versionf)) then $versionf() else 0, \n"
      + "let $repf := "
      + "  fn:function-lookup(xs:QName('hadoop:get-forest-replica-hosts'),2)\n"
      + "return exists($repf),"
      + "let $segRepf := "
      + "fn:function-lookup(xs:QName('hadoop:get-forest-replica-hosts-with-segment'),2)\n"
      + "return exists($segRepf),"
      + "let $f := "
      + "  fn:function-lookup(xs:QName('hadoop:get-assignment-policy'),0)\n"
      + "return if (exists($f)) then $f() else ()";
    // For HTTP Server
    public static final String HEADER_QUERY =
        "fn:exists(xdmp:get-request-header('x-forwarded-for'))";
    // For XDBC Server
    public static final String XDBC_HEADER_QUERY =
        "let $xdbcHeaderf := " +
        "fn:function-lookup(xs:QName('xdmp:get-xdbc-request-header'),1)\n" +
        "return if (exists($xdbcHeaderf)) " +
        "then fn:exists($xdbcHeaderf('x-forwarded-for')) else false()";
    
    protected AssignmentManager am = AssignmentManager.getInstance();
    protected boolean fastLoad;
    /** whether stats-based policy allows fastload **/
    protected boolean allowFastLoad = true;
    protected AssignmentPolicy.Kind policy;
    protected boolean legacy = false;
    protected boolean failover = false;
    // Added for backward compatibility issues with segment policy
    protected boolean supportSegment = false;
    protected String initHostName;

    @Override
    public void checkOutputSpecs(
        Configuration conf, ContentSource cs, JobContext context)
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
            String restrictHostsString = conf.getTrimmed(OUTPUT_RESTRICT_HOSTS);
            boolean restrictHosts = false;
            if (restrictHostsString!=null && !restrictHostsString.isEmpty()) {
                restrictHosts = Boolean.parseBoolean(restrictHostsString);
            }
            boolean getForwardHeader = restrictHostsString == null;
            
            // initialize server host name and assignment policy
            restrictHosts = initialize(session, restrictHosts, getForwardHeader);
            ((LocalJob)context).getThreadManager().setRestrictHosts(restrictHosts);
            
            // ensure manual directory creation 
            if (fastLoad) {
                LOG.info("Running in fast load mode");
                    // store forest-info map into config system
                DefaultStringifier.store(conf, queryForestInfo(cs),
                    OUTPUT_FOREST_HOST);

                AdhocQuery query = 
                		session.newAdhocQuery(DIRECTORY_CREATE_QUERY);
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
                TextArrayWritable hostArray = null;
                if (restrictHosts) {
                    String[] outputHosts = conf.getStrings(OUTPUT_HOST);
                    hostArray = new TextArrayWritable(outputHosts);
                } else {
                    String outputHost = cs.getConnectionProvider().getHostName();
                    // 23798: replace hostname in forest config with 
                    // user-specified output host
                    if (MODE_LOCAL.equals(conf.get(EXECUTION_MODE))) {
                        hostArray = queryHosts(cs, initHostName, outputHost);
                    } else {
                        hostArray = queryHosts(cs);
                    }
                }
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
                        !perm.equalsIgnoreCase(ContentCapability.UPDATE.toString()) &&
                        !perm.equalsIgnoreCase(ContentCapability.NODE_UPDATE.toString())) {
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
    
    protected Map<String, ContentSource> getSourceMap(boolean fastLoad, 
    		TaskAttemptContext context) throws IOException{
        Configuration conf = context.getConfiguration();
        Map<String, ContentSource> sourceMap =
            new LinkedHashMap<>();
        if (fastLoad) {
            LinkedMapWritable forestStatusMap = getForestStatusMap(conf);
            String[] outputHosts = conf.getStrings(OUTPUT_HOST);
            // Fastload import needs restrictHosts info from conf for 
            // multiple instances of ContentOutputFormat created by 
            // MultiThreadedMapper. It can't be saved as instance member 
            // because initialize() is only called once in LocalJobRunner
            boolean restrictHosts = conf.getBoolean(OUTPUT_RESTRICT_HOSTS, false);
            RestrictedHostsUtil rhUtil = null;
            if (restrictHosts) {
                rhUtil = new RestrictedHostsUtil(outputHosts);
                for (Writable forestId : forestStatusMap.keySet()) {
                    String forestHost = ((ForestInfo)forestStatusMap.get(forestId))
                            .getHostName();
                    rhUtil.addForestHost(forestHost);
                }
            }
            for (Writable forestId : forestStatusMap.keySet()) {
                ForestInfo fs = (ForestInfo)forestStatusMap.get(forestId);
                List<ForestHost> forestHostList = fs.getReplicas();
                for (ForestHost fh : forestHostList) {
                    String forestIdStr = fh.getForest();
                    String forestHost = fh.getHostName();
                    String targetHost = restrictHosts ?
                        rhUtil.getNextHost(forestHost) : forestHost;
                    if (fs.getUpdatable()) {
                        try {
                            ContentSource cs = sourceMap.get(targetHost);
                            if (cs == null) {
                                cs = InternalUtilities.getOutputContentSource(
                                    conf, targetHost);
                                sourceMap.put(targetHost, cs);
                            }
                            if (restrictHosts) {
                                sourceMap.put(forestHost, cs);
                            }
                        } catch (XccConfigException e) {
                            throw new IOException(e);
                        }
                    }
                }
            }
        } else {
            TextArrayWritable hosts = getHosts(conf);
            for (Writable host : hosts.get()) {
                String hostStr = host.toString();
                try {
                    ContentSource cs = InternalUtilities.getOutputContentSource(
                        conf, hostStr);
                    sourceMap.put(hostStr, cs);
                } catch (XccConfigException e) {
                    throw new IOException(e);
                }
            } 
        }
        return sourceMap;
    }
    
    @Override
    public RecordWriter<DocumentURI, VALUEOUT> getRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        // TODO: if MAPREDUCE-3377 still exists, need to re-run initialize
        fastLoad = Boolean.valueOf(conf.get(OUTPUT_FAST_LOAD));
        Map<String, ContentSource> sourceMap = getSourceMap(fastLoad, context);
        // construct the ContentWriter
        return new ContentWriter<>(conf, sourceMap, fastLoad,
            am);
    }

    protected LinkedMapWritable getForestStatusMap(Configuration conf) 
    throws IOException {
        String forestHost = conf.get(OUTPUT_FOREST_HOST);
        if (forestHost != null) {
            //Restores the object from the configuration.
            LinkedMapWritable fhmap = DefaultStringifier.load(conf, OUTPUT_FOREST_HOST, 
                LinkedMapWritable.class);
            // must be in fast load mode, otherwise won't reach here
            String s = conf.get(ASSIGNMENT_POLICY);
            //EXECUTION_MODE must have a value in mlcp;
            //default is "distributed" in hadoop connector
            String mode = conf.get(EXECUTION_MODE, MODE_DISTRIBUTED);
            if (MODE_DISTRIBUTED.equals(mode)) {
            	AssignmentPolicy.Kind policy =
            			AssignmentPolicy.Kind.forName(s);
                am.initialize(policy, fhmap, conf.getInt(BATCH_SIZE, 10));
            }
            return fhmap;
        } else {
            throw new IOException("Forest host map not found");
        }
    }
    
    /**
     * Initialize initial server host name, assignment policy and fastload.
     * 
     * @param session
     * @return true if restrict_hosts is to be enabled; false otherwise
     * @throws IOException
     * @throws RequestException
     */
    protected boolean initialize(Session session, boolean restrictHosts, 
            boolean getForwardHeader)
        throws IOException, RequestException {
        String queryText = INIT_QUERY;
        if (getForwardHeader) {
            StringBuilder buf = new StringBuilder();
            buf.append(HEADER_QUERY).append(";\n");
            buf.append(XDBC_HEADER_QUERY).append(";\n");
            buf.append(queryText);
            queryText = buf.toString();
        }
        AdhocQuery query = session.newAdhocQuery(queryText);
        RequestOptions options = new RequestOptions();
        options.setDefaultXQueryVersion("1.0-ml");
        query.setOptions(options);
        ResultSequence result = null;
        if (LOG.isDebugEnabled()) {
            LOG.debug("init query: \n" + query.getQuery());
        }
        result = session.submitRequest(query);

        ResultItem item = result.next();
        boolean httpForwardHeaderExists;
        boolean xdbcForwardHeaderExists;
        if (getForwardHeader) {
            httpForwardHeaderExists = item.asString().equals("true");
            item = result.next();
            xdbcForwardHeaderExists = item.asString().equals("true");
            item = result.next();
            if (httpForwardHeaderExists || xdbcForwardHeaderExists) {
                restrictHosts = true;
                conf.setBoolean(OUTPUT_RESTRICT_HOSTS, true);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("HTTP compliant mode enabled since x-forwarded-for exists");
                }
            } else {
                // check if input needs to be in HTTP compliant mode
                String inputRestrictHost = conf.getTrimmed(INPUT_RESTRICT_HOSTS);
                if (inputRestrictHost == null || 
                    inputRestrictHost.equalsIgnoreCase("false")) {
                    HttpChannel.setUseHTTP(false);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("HTTP compliant mode disabled since x-forwarded-for doesn't exist");
                    }
                }
            }
        }
        initHostName = item.asString();
        item = result.next();
        am.setEffectiveVersion(((XSInteger)item.getItem()).asLong());
        item = result.next();
        failover = !restrictHosts && item.asString().equals("true");
        item = result.next();
        supportSegment = item.asString().equals("true");
        if (result.hasNext()) {
            item = result.next();
            String policyStr = item.asString();
            conf.set(ASSIGNMENT_POLICY, policyStr);
            policy = AssignmentPolicy.Kind.forName(policyStr);
            item = result.next();
            allowFastLoad = Boolean.parseBoolean(item.asString());
            if ((policy == AssignmentPolicy.Kind.STATISTICAL 
                || policy == AssignmentPolicy.Kind.RANGE
                || policy == AssignmentPolicy.Kind.QUERY)
                && !allowFastLoad && conf.getBoolean(OUTPUT_FAST_LOAD, false)) {
                throw new IOException(
                    "Fastload can't be used: rebalancer is on and "
                        + "forests are imbalanced in a database with "
                        + "statistics-based assignment policy");
            }
        } else {
        	policy = AssignmentPolicy.Kind.LEGACY;
        	legacy = true;
        }
        
        // initialize fastload mode
        if (conf.get(OUTPUT_FAST_LOAD) == null) {
            // fastload not set
            if (conf.get(OUTPUT_DIRECTORY) != null) {
                // output_dir is set, attempt to do fastload
                if(conf.get(OUTPUT_PARTITION) == null && 
                   (policy == AssignmentPolicy.Kind.RANGE ||
                    policy == AssignmentPolicy.Kind.QUERY)) {
                    fastLoad = false;
                } else if (policy == AssignmentPolicy.Kind.RANGE ||
                           policy == AssignmentPolicy.Kind.QUERY ||
                	   policy == AssignmentPolicy.Kind.STATISTICAL) {
                    fastLoad = allowFastLoad;
                } else {
                	fastLoad = true;
                }
            } else {
                //neither fastload nor output_dir is set
                fastLoad = false;
            }
        } else {
            fastLoad = conf.getBoolean(OUTPUT_FAST_LOAD, false);
            if (fastLoad && conf.get(OUTPUT_PARTITION) == null
                && (policy == AssignmentPolicy.Kind.RANGE ||
                    policy == AssignmentPolicy.Kind.QUERY)) {
                throw new IllegalArgumentException(
                    "output_partition is required for fastload mode.");
            }
        }
        conf.setBoolean(OUTPUT_FAST_LOAD, fastLoad);
        return restrictHosts;
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
            if (legacy) {
                LOG.debug("Legacy assignment is assumed for older MarkLogic" + 
                          " Server.");
                query = session.newAdhocQuery(FOREST_HOST_MAP_QUERY);
            } else {
                /* 
                 * failover if restrict host is not set and the server is 9.0 
                 * we need the failover forests and hosts for failover
                 */
                if (failover) {
                    if (supportSegment) {
                        query = session.newAdhocQuery(
                        FOREST_REPLICA_HOST_QUERY_WITH_SEGMENT);
                    } else {
                        query = session.newAdhocQuery(FOREST_REPLICA_HOST_QUERY);
                    }
                } else {
                  query = session.newAdhocQuery(FOREST_HOST_QUERY);
                }
                if (policy == AssignmentPolicy.Kind.RANGE ||
                    policy == AssignmentPolicy.Kind.QUERY) {
                    String pName = conf.get(OUTPUT_PARTITION);
                    query.setNewStringVariable("partition-name", pName);
                } else {
                    query.setNewStringVariable("partition-name", "");
                }
                query.setNewStringVariable("policy", 
                		policy.toString().toLowerCase());
            }

            // query forest status mapping                 
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");
            query.setOptions(options);
            if(LOG.isDebugEnabled()) {
                LOG.debug(query.getQuery());
            }
            result = session.submitRequest(query);

            LinkedMapWritable forestStatusMap = new LinkedMapWritable();
            Text forest = null;
            List<ForestHost> replicas = new ArrayList<>();
            String outputHost = cs.getConnectionProvider().getHostName();
            boolean local = MODE_LOCAL.equals(conf.get(EXECUTION_MODE));
            
            while (result.hasNext()) {
                ResultItem item = result.next();
                if (forest == null) {
                    forest = new Text(item.asString());
                } else {
                    String hostName = item.asString();
                    // 23798: replace hostname in forest config with 
                    // user-specified output host
                    if (local && hostName != null && 
                        hostName.equals(initHostName)) {
                    	hostName = outputHost;
                    }
                    boolean updatable = true;
                    long dc = -1;
                    if (!legacy) {
                        if (policy == AssignmentPolicy.Kind.BUCKET ||
                            policy == AssignmentPolicy.Kind.SEGMENT && supportSegment) {
                            item = result.next();
                            updatable = Boolean.parseBoolean(item
                                .asString());
                        } else if (policy == AssignmentPolicy.Kind.RANGE ||
                                   policy == AssignmentPolicy.Kind.STATISTICAL ||
                                   policy == AssignmentPolicy.Kind.QUERY ) {
                            // range or statistical
                            item = result.next();
                            dc = Long.parseLong(item.asString());
                        }
                    } 
                    if (failover) {
                        String curForest = "";
                        String curHost = "";
                        int count = 0;
                        while (result.hasNext()) {
                            item = result.next();
                            if (ItemType.XS_INTEGER == item.getItemType()) {
                                if (((XSInteger)item.getItem()).asPrimitiveInt() == 0) {
                                    break;
                                }
                            }
                            int index = count % 2;
                            if (index == 0) {
                                curForest = item.asString();
                            } else if (index == 1) {
                                curHost = item.asString();
                                ForestHost info = new ForestHost(curForest, curHost);
                                replicas.add(info);
                            }
                            count++;
                        } 
                    } else {
                        ForestHost info = new ForestHost(forest.toString(), hostName);
                        replicas.add(info);
                    }
                    forestStatusMap.put(forest, new ForestInfo(
                        hostName, dc, updatable, replicas));
                    forest = null;
                    replicas.clear();
                }
            }
            if (forestStatusMap.size() == 0) {
                throw new IOException("Target database has no forests attached: "
                    + "check forests in database");
            }
            am.initialize(policy, forestStatusMap, conf.getInt(BATCH_SIZE,10));
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

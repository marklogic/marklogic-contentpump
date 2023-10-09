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

package com.marklogic.contentpump;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.marklogic.xcc.exceptions.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.io.Base64;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.ContentWriter;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicCounter;
import com.marklogic.mapreduce.MarkLogicDocument;
import com.marklogic.mapreduce.ZipEntryInputStream;
import com.marklogic.mapreduce.utilities.AssignmentManager;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.DocumentRepairLevel;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.Session.TransactionMode;
import com.marklogic.xcc.ValueFactory;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XName;
import com.marklogic.xcc.types.XdmValue;

/**
 * ContentWriter that does server-side transform and insert
 * @author ali
 *
 * @param <VALUEOUT>
 */
public class TransformWriter<VALUEOUT> extends ContentWriter<VALUEOUT> {
    public static final Log LOG = LogFactory.getLog(TransformWriter.class);
    static final long TRANS_OPT_MIN_VERSION = 9000200;
    static final long PROPS_MIN_VERSION = 9000400;
    static final String MAP_ELEM_START_TAG = 
        "<map:map xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi"
        + "=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:map=\"http:"
        + "//marklogic.com/xdmp/map\">";
    protected String moduleUri;
    protected String functionNs;
    protected String functionName;
    protected String functionParam;
    protected XdmValue transOpt;
    protected ContentType contentType;
    protected AdhocQuery[] queries;
    // Please note the name diff between pendingURIs here and pendingUris in ContentWriter
    protected Set<DocumentURI>[] pendingURIs;
    protected XdmValue[][] uris;
    protected XdmValue[][] values;
    protected XdmValue[][] optionsVals;
    protected HashMap<String, String> optionsMap;
    protected XName uriName;
    protected XName contentName;
    protected XName optionsName;
    protected XName transOptName;
    protected String query;

    public TransformWriter(Configuration conf,
        Map<String, ContentSource> hostSourceMap, boolean fastLoad,
        AssignmentManager am) {
        super(conf, hostSourceMap, fastLoad, am);

        batchSize = effectiveVersion >= ConfigConstants.BATCH_MIN_VERSION ? batchSize : 1;
        moduleUri = conf.get(ConfigConstants.CONF_TRANSFORM_MODULE);
        functionNs = conf.get(ConfigConstants.CONF_TRANSFORM_NAMESPACE, "");
        functionName = conf.get(ConfigConstants.CONF_TRANSFORM_FUNCTION,
            "transform");
        functionParam = conf.get(ConfigConstants.CONF_TRANSFORM_PARAM, "");
        String contentTypeStr = conf.get(MarkLogicConstants.CONTENT_TYPE,
            MarkLogicConstants.DEFAULT_CONTENT_TYPE);
        contentType = ContentType.valueOf(contentTypeStr);
        queries = new AdhocQuery[sessions.length];
        
        pendingURIs = new HashSet[sessions.length];
        for (int i = 0; i < sessions.length; i++) {
            pendingURIs[i] = new HashSet<>(batchSize);
        }
        // counts is only initialized by ContentWriter when batchSize > 1
        if (counts == null) {
            counts = new int[sessions.length];
        }
        // Whether the server mlcp talks to has a transformation-option
        // in transformation-insert-batch signature
        boolean hasOpt = effectiveVersion >= TRANS_OPT_MIN_VERSION;
        uris = new XdmValue[counts.length][batchSize];
        values = new XdmValue[counts.length][batchSize];
        optionsVals = new XdmValue[counts.length][batchSize];
        optionsMap = new HashMap<>();
        uriName = new XName("URI");
        contentName = new XName("CONTENT");
        optionsName = new XName("INSERT-OPTIONS");
        query = constructQryString(moduleUri, functionNs,
                functionName, functionParam, effectiveVersion, hasOpt);
        if (hasOpt) {
            transOptName = new XName("TRANSFORM-OPTION");
            transOpt = constructTransformOption(conf,
                    functionParam, functionNs);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("query:"+query);
        }
    }
    
    private static XdmValue constructTransformOption(Configuration conf,
            String functionParam, String functionNs) {
        HashMap<String, String> transMap = new HashMap<>();
        String modules = conf.get(ConfigConstants.CONF_INPUT_MODULES_DATABASE);
        String modulesRoot = conf.get(ConfigConstants.CONF_INPUT_MODULES_ROOT);
        if (modules != null) {
            transMap.put("modules", modules);
        }
        if (modulesRoot != null) {
            transMap.put("modules-root", modulesRoot);
        }
        if (!"".equals(functionNs)) {
            transMap.put("transform-namespace", functionNs);
        }
        if (!"".equals(functionParam)) {
            transMap.put("transform-param", functionParam);
        }
        if (transMap.isEmpty()) {
            return ValueFactory.newJSNull();
        } else {
            ObjectNode node = mapToNode(transMap);
            return ValueFactory.newValue(ValueType.JS_OBJECT, node);
        }
    }
    
    private static String constructQryString(String moduleUri, 
            String functionNs, String functionName,
            String functionParam, long effectiveVersion, boolean hasOpt) {
        StringBuilder q = new StringBuilder();
        q.append("xquery version \"1.0-ml\";\n")
        .append("import module namespace hadoop = \"http://marklogic.com")
        .append("/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n")
        .append("declare variable $URI as xs:string* external;\n")
        .append("declare variable $CONTENT as item()* external;\n")
        .append("declare variable $INSERT-OPTIONS as ");
        if (effectiveVersion < ConfigConstants.BATCH_MIN_VERSION) {
            q.append("element() external;\nhadoop:transform-and-insert(\"");
        } else {
            q.append("map:map* external;\n");
            if (hasOpt) {
                q.append("declare variable $TRANSFORM-OPTION as map:map? external;\n");
            }
            q.append("hadoop:transform-insert-batch(\"");
        }
        q.append(moduleUri).append("\",\"");
        if (!hasOpt) {
            q.append(functionNs).append("\",\"");
        }
        q.append(functionName).append("\", ");
        if (!hasOpt) {
            q.append("\"")
            .append(functionParam.replace("\"", "\"\""))
            .append("\", ");
        } else {
            q.append("$TRANSFORM-OPTION, ");
        }
        q.append("$URI, $CONTENT, $INSERT-OPTIONS");
        q.append(")");
        return q.toString();
    }

    @Override
    public void write(DocumentURI key, VALUEOUT value) throws IOException,
        InterruptedException {
        int fId = 0;
        String uri = InternalUtilities.getUriWithOutputDir(key, outputDir);
        if (fastLoad) {
            if (!countBased) {
                // placement for legacy or bucket
                fId = am.getPlacementForestIndex(key);
                sfId = fId;
            } else {
                if (sfId == -1) {
                    sfId = am.getPlacementForestIndex(key);
                }
                fId = sfId;
            }
        }
        int sid = fId;
        addValue(uri, value, sid, options, null);
        pendingURIs[sid].add((DocumentURI)key.clone());
        boolean committed = false;
        if (++counts[sid] == batchSize) {
            commitRetry = 0;
            commitSleepTime = MIN_SLEEP_TIME;
            if (sessions[sid] == null) {
                sessions[sid] = getSession(sid, false);
                queries[sid] = getAdhocQuery(sid);
            }
            while (commitRetry < commitRetryLimit) {
                committed = false;
                if (commitRetry > 0) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(getFormattedBatchId() +
                            "Retrying committing batch, attempts: " +
                            commitRetry + "/" + MAX_RETRIES);
                    }
                }
                queries[sid].setNewVariables(uriName, uris[sid]);
                queries[sid].setNewVariables(contentName, values[sid]);
                queries[sid].setNewVariables(optionsName, optionsVals[sid]);
                try {
                    insertBatch(sid, uris[sid], values[sid], optionsVals[sid]);
                } catch (Exception e) {}
                stmtCounts[sid]++;
                //reset forest index for statistical
                if (countBased) {
                    sfId = -1;
                }
                counts[sid] = 0;

                if (needCommit && stmtCounts[sid] == txnSize) {
                    try {
                        commit(sid);
                        if (commitRetry > 0) {
                            LOG.info(getFormattedBatchId() +
                                "Retrying committing batch is successful");
                        }
                    } catch (Exception e) {
                        boolean isRetryable = true;
                        LOG.warn("Failed committing transaction.");
                        if (e instanceof MLCloudRequestException){
                            isRetryable = ((MLCloudRequestException)e).isRetryable();
                            LOG.warn(getFormattedBatchId() +
                                "MLCloudRequestException:" + e.getMessage());
                        } else {
                            LOG.warn(getFormattedBatchId() +
                                "Exception:" + e.getMessage());
                        }
                        if (isRetryable && needCommitRetry() &&
                            (++commitRetry < commitRetryLimit)) {
                            LOG.warn(getFormattedBatchId() + "Failed during committing");
                            handleCommitExceptions(sid);
                            commitSleepTime = sleep(commitSleepTime);
                            stmtCounts[sid] = 0;
                            sessions[sid] = getSession(sid, true);
                            queries[sid] = getAdhocQuery(sid);
                            continue;
                        } else if (needCommitRetry()) {
                            LOG.error(getFormattedBatchId() +
                                "Exceeded max commit retry, batch failed permanently");
                        }
                        failed += commitUris[sid].size();
                        for (DocumentURI failedUri : commitUris[sid]) {
                            LOG.error(getFormattedBatchId() +
                                "Document failed permanently: " + failedUri);
                        }
                        handleCommitExceptions(sid);
                    }
                    finally {
                        stmtCounts[sid] = 0;
                        committed = true;
                    }
                }
                break;
            }
            batchId++;
            pendingURIs[sid].clear();
        }
        if ((!fastLoad) && ((!needCommit) || committed)) {
            // rotate to next host and reset session
            hostId = (hostId + 1)%forestIds.length;
            sessions[0] = null;
            queries[0] = null;
        }
    }
    
    private static String getTypeFromMap(String uri) {
        int idx = uri.lastIndexOf(".");
        Text format = null;
        if (idx != -1) {
            String suff = uri.substring(idx + 1, uri.length());
            if (suff.equalsIgnoreCase("xml"))
                return "xml";
            format = (Text) TransformOutputFormat.mimetypeMap.get(new Text(suff));
        }
        if (format == null) {
            return "binary";
        } else {
            return format.toString();
        }
    }

    public static ObjectNode mapToNode(HashMap<String, String> optionsMap) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        for (Map.Entry<String, String> entry : optionsMap.entrySet()) {
            node.put(entry.getKey(), entry.getValue());
        }
        return node;
    }

    public static String mapToElement(HashMap<String, String> map) {
        StringBuilder sb = new StringBuilder();
        sb.append(MAP_ELEM_START_TAG);
        Set<String> keys = map.keySet();
        for (String k : keys) {
            addKeyValue(sb, k, map.get(k));
        }
        sb.append("</map:map>");
        return sb.toString();
    }

    private static void addKeyValue(StringBuilder sb, String key, String value) {
        sb.append("<map:entry key=\"").append(key)
            .append("\"><map:value xsi:type=\"xs:string\">").append(value)
            .append("</map:value></map:entry>");
    }
    
    protected void addValue(String uri, VALUEOUT value, int id, 
        ContentCreateOptions options, String properties) throws UnsupportedEncodingException {
        uris[id][counts[id]] = ValueFactory.newValue(ValueType.XS_STRING,uri);
        ContentType docContentType = contentType;
        if (options.getFormat() != DocumentFormat.NONE) {
            docContentType = ContentType.fromFormat(options.getFormat());
        } else if (contentType == ContentType.MIXED) {
            // get type from mimetype map
            docContentType = ContentType.forName(getTypeFromMap(uri));
        }

        switch (docContentType) {
        case BINARY:
            if (value instanceof MarkLogicDocument) {
                values[id][counts[id]] = 
                    ValueFactory.newValue(ValueType.XS_BASE64_BINARY, 
                        Base64.encodeBytes(
                          ((MarkLogicDocument)value).getContentAsByteArray()));
            } else {
                values[id][counts[id]] = 
                    ValueFactory.newValue(ValueType.XS_BASE64_BINARY, 
                        Base64.encodeBytes(((BytesWritable)value).getBytes(),
                            0, ((BytesWritable)value).getLength()));
            }
            optionsMap.put("value-type", 
                    ValueType.XS_BASE64_BINARY.toString());
            break;
                    
        case TEXT:
            if (value instanceof BytesWritable) {
                // in MIXED type, value is byteswritable
                String encoding = options.getEncoding();
                values[id][counts[id]] = 
                    ValueFactory.newValue(ValueType.XS_STRING, 
                        new String(((BytesWritable) value).getBytes(), 0,
                            ((BytesWritable) value).getLength(), encoding));
            } else if (value instanceof MarkLogicDocument) {
                values[id][counts[id]] = 
                        ValueFactory.newValue(ValueType.XS_STRING, 
                            ((MarkLogicDocument)value).getContentAsString());
            } else {
                // must be text or xml
                values[id][counts[id]] = 
                    ValueFactory.newValue(ValueType.XS_STRING, 
                        ((Text) value).toString());
            }
            optionsMap.put("value-type", ValueType.TEXT.toString());
            break;
        case JSON:
        case XML:
            if (value instanceof BytesWritable) {
                // in MIXED type, value is byteswritable
                String encoding = options.getEncoding();
                values[id][counts[id]] = 
                    ValueFactory.newValue(ValueType.XS_STRING, 
                        new String(((BytesWritable) value).getBytes(), 0,
                            ((BytesWritable) value).getLength(), encoding));
            } else if (value instanceof RDFWritable) {
                //RDFWritable's value is Text
                values[id][counts[id]] = 
                    ValueFactory.newValue(ValueType.XS_STRING, 
                        ((RDFWritable)value).getValue().toString());
            } else if (value instanceof ContentWithFileNameWritable) {
                values[id][counts[id]] = 
                    ValueFactory.newValue(ValueType.XS_STRING, 
                    ((ContentWithFileNameWritable)value).getValue().toString());
            } else if (value instanceof MarkLogicDocument) {
                values[id][counts[id]] = 
                    ValueFactory.newValue(ValueType.XS_STRING, 
                            ((MarkLogicDocument)value).getContentAsString());
            } else {
                // must be text or xml
                values[id][counts[id]] = 
                    ValueFactory.newValue(ValueType.XS_STRING,
                        ((Text) value).toString());
            }
            optionsMap.put("value-type", ValueType.XS_STRING.toString());
            break;
        case MIXED:
        case UNKNOWN:
            throw new RuntimeException("Unexpected:" + docContentType);
        default:
            throw new UnsupportedOperationException("invalid type:"
                + docContentType);
        }
        String namespace = options.getNamespace();
        if (namespace != null) {
            optionsMap.put("namespace", namespace);
        }
        String lang = options.getLanguage();
        if (lang != null) {
            optionsMap.put("language", "default-language=" + lang);
        }
        ContentPermission[] perms = options.getPermissions();
        StringBuilder rolesReadList = new StringBuilder();
        StringBuilder rolesExeList = new StringBuilder();
        StringBuilder rolesUpdateList = new StringBuilder();
        StringBuilder rolesInsertList = new StringBuilder();
        StringBuilder rolesNodeUpdateList = new StringBuilder();
        if (perms != null && perms.length > 0) {
            for (ContentPermission cp : perms) {
                String roleName = cp.getRole();
                if (roleName == null || roleName.isEmpty()) {
                    LOG.error("Illegal role name: " + roleName);
                    continue;
                }
                ContentCapability cc = cp.getCapability();
                if (cc.equals(ContentCapability.READ)) {
                    if (rolesReadList.length() != 0) {
                        rolesReadList.append(",");
                    }
                    rolesReadList.append(roleName);
                } else if (cc.equals(ContentCapability.EXECUTE)) {
                    if (rolesExeList.length() != 0) {
                        rolesExeList.append(",");
                    }
                    rolesExeList.append(roleName);
                } else if (cc.equals(ContentCapability.INSERT)) {
                    if (rolesInsertList.length() != 0) {
                        rolesInsertList.append(",");
                    }
                    rolesInsertList.append(roleName);
                } else if (cc.equals(ContentCapability.UPDATE)) {
                    if (rolesUpdateList.length() != 0) {
                        rolesUpdateList.append(",");
                    }
                    rolesUpdateList.append(roleName);
                } else if (cc.equals(ContentCapability.NODE_UPDATE)) {
                    if (rolesNodeUpdateList.length() != 0) {
                        rolesNodeUpdateList.append(",");
                    }
                    rolesNodeUpdateList.append(roleName);
                }
            }
        }
        optionsMap.put("roles-read", rolesReadList.toString());
        optionsMap.put("roles-execute", rolesExeList.toString());
        optionsMap.put("roles-update", rolesUpdateList.toString());
        optionsMap.put("roles-insert", rolesInsertList.toString());
        optionsMap.put("roles-node-update", rolesNodeUpdateList.toString());

        String[] collections = options.getCollections();
        StringBuilder sb = new StringBuilder();
        if (collections != null || value instanceof ContentWithFileNameWritable) {
            if (collections != null) {
                for (int i = 0; i < collections.length; i++) {
                    if (i != 0)
                        sb.append(",");
                    sb.append(collections[i].trim());
                }
            } 
                
            if (value instanceof ContentWithFileNameWritable) {
                if(collections != null)
                    sb.append(",");
                sb.append(((ContentWithFileNameWritable) value).getFileName());
            }
            
            optionsMap.put("collections", sb.toString());
        }

        optionsMap.put("quality", String.valueOf(options.getQuality()));
        DocumentRepairLevel repairLevel = options.getRepairLevel();
        if (!DocumentRepairLevel.DEFAULT.equals(repairLevel)) {
            optionsMap.put("xml-repair-level", "repair-" + repairLevel);
        }

        String temporalCollection = options.getTemporalCollection();
        if (temporalCollection != null) {
            optionsMap.put("temporal-collection", temporalCollection);
        }

        if (properties != null) {
            optionsMap.put("properties", properties);
        }

        Map<String, String> meta = options.getMetadata();
        if (meta != null && meta instanceof HashMap) {
            HashMap<String, String> hMeta = (HashMap<String, String>) meta;
            optionsMap.put("metadata", mapToNode(hMeta).toString());
        }

        if (effectiveVersion < ConfigConstants.BATCH_MIN_VERSION) {
            String optionElem = mapToElement(optionsMap);
            optionsVals[id][counts[id]] = 
                    ValueFactory.newValue(ValueType.ELEMENT, optionElem);
        } else {
            ObjectNode optionsNode = mapToNode(optionsMap);
            optionsVals[id][counts[id]] = 
                    ValueFactory.newValue(ValueType.JS_OBJECT, optionsNode);
        }
        optionsMap.clear();
    }

    protected Session getSession(int fId, boolean nextReplica) {
        TransactionMode mode = TransactionMode.AUTO;
        if (needCommit) {
            mode = TransactionMode.UPDATE;
        }
        return getSession(fId, nextReplica, mode);
    }
    
    protected AdhocQuery getAdhocQuery(int sid) {
        AdhocQuery q = sessions[sid].newAdhocQuery(query);
        RequestOptions rOptions = new RequestOptions();
        rOptions.setDefaultXQueryVersion("1.0-ml");
        q.setOptions(rOptions);
        return q;
    }
    
    protected void insertBatch(int id, XdmValue[] uriList, 
            XdmValue[] valueList, XdmValue[] optionsValList) 
    throws IOException
    {
        batchRetry = 0;
        batchSleepTime = MIN_SLEEP_TIME;
        while (batchRetry < MAX_RETRIES) {
        try {
            if (batchRetry > 0) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(getFormattedBatchId() +
                        "Retrying inserting batch, attempts: " + batchRetry +
                        "/" + MAX_RETRIES);
                }
            }
            if (transOpt != null) {
                queries[id].setNewVariable(transOptName, transOpt);
            }
            ResultSequence rs = sessions[id].submitRequest(queries[id]);
            while (rs.hasNext()) { // batch mode
                String uri = rs.next().asString();
                if (pendingURIs[id].remove(new DocumentURI(uri))) {
                    LOG.error(getFormattedBatchId() +
                        "Document failed permanently: " + uri);
                    failed++;
                }
                if (rs.hasNext()) {
                    String err = rs.next().asString();
                    LOG.warn(err);
                } else break;   
            }
            if (batchRetry > 0) {
                LOG.info(getFormattedBatchId() +
                    "Retrying inserting batch is successful");
            }
        } catch (Exception e) {
            boolean retryable = true;
            // compatible mode: log error and continue
            if (e instanceof QueryException) {
                LOG.warn(getFormattedBatchId() + "QueryException: " +
                        ((QueryException) e).getFormatString());
                retryable = ((QueryException) e).isRetryable();
            } else if (e instanceof RequestServerException) {
                LOG.warn(getFormattedBatchId() + "RequestServerException:" + e.getMessage());
            } else {
                LOG.warn(getFormattedBatchId() + "Exception: " + e.getMessage());
                if (e.getMessage().contains("Module Not Found")) {
                    retryable = false;
                }
            }
            LOG.warn(getFormattedBatchId() + "Failed during inserting");
            if (needCommit) {
                rollback(id);
            }

            if (retryable && ++batchRetry < MAX_RETRIES) {
                sessions[id].close();
                batchSleepTime = sleep(batchSleepTime);
                sessions[id] = getSession(id, true);
                queries[id] = getAdhocQuery(id);
                queries[id].setNewVariables(uriName, uriList);
                queries[id].setNewVariables(contentName, valueList);
                queries[id].setNewVariables(optionsName, optionsValList);
                continue;
            } else if (retryable) {
                LOG.error(getFormattedBatchId() +
                    "Exceeded max batch retry, batch failed permanently");
            }
            for ( DocumentURI failedUri: pendingURIs[id] ) {
                LOG.error(getFormattedBatchId() +
                    "Document failed permanently: " + failedUri);
                failed++;
            }
            pendingURIs[id].clear();
            throw new IOException(e);
        }
        break;
        }
        if (needCommit) {
            commitUris[id].addAll(pendingURIs[id]);
        } else {
            succeeded += pendingURIs[id].size();
        }
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException,
    InterruptedException {
        for (int i = 0; i < sessions.length; i++) {
            if (pendingURIs[i].size() > 0) {
                commitRetry = 0;
                commitSleepTime = MIN_SLEEP_TIME;
                if (sessions[i] == null) {
                    sessions[i] = getSession(i, false);
                    queries[i] = getAdhocQuery(i);
                }
                XdmValue[] urisLeft = new XdmValue[counts[i]];
                System.arraycopy(uris[i], 0, urisLeft, 0, counts[i]);
                XdmValue[] valuesLeft = new XdmValue[counts[i]];
                System.arraycopy(values[i], 0, valuesLeft, 0, counts[i]);
                XdmValue[] optionsLeft = new XdmValue[counts[i]];
                System.arraycopy(optionsVals[i], 0, optionsLeft, 0, counts[i]);

                while (commitRetry < commitRetryLimit) {
                    if (commitRetry > 0) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(getFormattedBatchId() +
                                "Retrying committing batch, attempts: " +
                                commitRetry + "/" + MAX_RETRIES);
                        }
                    }
                    queries[i].setNewVariables(uriName, urisLeft);
                    queries[i].setNewVariables(contentName, valuesLeft);
                    queries[i].setNewVariables(optionsName, optionsLeft);
                    try {
                        insertBatch(i, urisLeft, valuesLeft, optionsLeft);
                    } catch (Exception e) {
                    }
                    stmtCounts[i]++;

                    if (stmtCounts[i] > 0 && needCommit) {
                        try {
                            commit(i);
                            if (commitRetry > 0) {
                                LOG.info(getFormattedBatchId() +
                                    "Retrying committing batch is successful");
                            }
                        } catch (Exception e) {
                            LOG.warn(getFormattedBatchId() +
                                "Failed committing transaction: " + e.getMessage());
                            if (needCommitRetry() && ++commitRetry < commitRetryLimit) {
                                LOG.warn(getFormattedBatchId() + "Failed during committing");
                                handleCommitExceptions(i);
                                commitSleepTime = sleep(commitSleepTime);
                                sessions[i] = getSession(i, true);
                                stmtCounts[i] = 0;
                                continue;
                            } else if (needCommitRetry()) {
                                LOG.error(getFormattedBatchId() +
                                    "Exceeded max commit retry, batch failed permanently");
                            }
                            failed += commitUris[i].size();
                            for (DocumentURI failedUri : commitUris[i]) {
                                LOG.error(getFormattedBatchId() +
                                    "Document failed permanently: " + failedUri);
                            }
                            handleCommitExceptions(i);
                        } finally {
                            stmtCounts[i] = 0;
                            sessions[i].close();
                        }
                    }
                    break;
                }
                batchId++;
                pendingURIs[i].clear();
            }
        }

        closeSessions();

        if (is != null) {
            is.close();
            if (is instanceof ZipEntryInputStream) {
                ((ZipEntryInputStream)is).closeZipInputStream();
            }
        }
        Counter committedCounter = context.getCounter(
                MarkLogicCounter.OUTPUT_RECORDS_COMMITTED);
        synchronized(committedCounter) {
            committedCounter.increment(succeeded);
        }
        Counter failedCounter = context.getCounter(
                MarkLogicCounter.OUTPUT_RECORDS_FAILED);
        synchronized(failedCounter) {
            failedCounter.increment(failed);
        }
    }
}

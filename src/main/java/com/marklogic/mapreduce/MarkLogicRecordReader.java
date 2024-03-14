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
import java.math.BigInteger;
import java.util.List;
import java.util.Collection;
import java.util.Iterator;

import com.marklogic.xcc.exceptions.MLCloudRequestException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.mapreduce.functions.LexiconFunction;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.mapreduce.utilities.ForestHost;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.QueryException;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
/**
 * A RecordReader that fetches data from MarkLogic server and generates 
 * &lt;K, V&gt; key value pairs.
 * 
 * @author jchen
 * 
 * @param <KEYIN>
 * @param <VALUEIN>
 */
public abstract class MarkLogicRecordReader<KEYIN, VALUEIN> 
extends RecordReader<KEYIN, VALUEIN>
implements MarkLogicConstants {

    public static final Log LOG =
        LogFactory.getLog(MarkLogicRecordReader.class);
    
    /**
     * Input split for this record reader
     */
    protected MarkLogicInputSplit mlSplit;
    /**
     * Count of records fetched
     */
    protected long count;
    /**
     * Session to the MarkLogic server.
     */
    protected Session session;
    /**
     * ResultSequence from the MarkLogic server.
     */
    protected ResultSequence result;
    /**
     * Job configuration.
     */
    protected Configuration conf;
    /**
     * Total expected count of the records in a split.
     */
    protected float length;
    /**
     * Redaction rule collection names.
     */
    protected String[] redactionRuleCol;
    /**
     * list of forests for failover
     */   
    protected List<ForestHost> replicas;
    /**
     * Current Forest to connect for failover 
     */
    protected int curForest;
    /**
     * Hostnames from the input split
     */
    protected String[] hostNames;
    /**
     * for failover retry
     */
    protected int retry;

    protected final int maxRetries = 15;

    protected int sleepTime;

    protected final int maxSleepTime = 30000;

    public MarkLogicRecordReader(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void close() throws IOException {
        if (result != null) {
            result.close();
        }
        if (session != null) {
            session.close();
        }
    }

    public Configuration getConf() {
        return conf;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return count/length;
    }
    
    private void appendNamespace(Collection<String> nsCol, StringBuilder buf) {
        for (Iterator<String> nsIt = nsCol.iterator(); 
                nsIt.hasNext();) {
            String ns = nsIt.next();
            buf.append('"').append(ns).append('"');
            if (nsIt.hasNext()) {
                buf.append(',');
            }
        }
    }
    
    protected void buildSrcInDocExprQuery(String docExpr, String subExpr, StringBuilder buf) {
        buf.append("fn:unordered(fn:unordered(");
        buf.append(docExpr == null ? "fn:collection()" : docExpr);
        buf.append(")[$mlmr:splitstart to $mlmr:splitend]");
        buf.append(subExpr == null ? "" : subExpr);
        buf.append(")");
    }
    
    protected void buildDocExprQuery(String docExpr, Collection<String> nsCol, 
            String subExpr, StringBuilder buf) {
        buf.append("xdmp:with-namespaces(("); 
        if (nsCol != null) {
            appendNamespace(nsCol, buf);
        }
        buf.append("),");        
        
        if (redactionRuleCol != null) {
        	buf.append("rdt:redact(");
        	buildSrcInDocExprQuery(docExpr, subExpr, buf);
        	buf.append(",((");
        	
        	for (int i = 0; i < redactionRuleCol.length; i++) {
        		if (i != 0) {
        			buf.append(",");
        		}
        		buf.append("\"" + redactionRuleCol[i] + "\"");
        	}
        	
        	buf.append(")))");
        } else {
            buildSrcInDocExprQuery(docExpr, subExpr, buf);
        }
        buf.append(")");
    }
    
    protected void buildSrcInSearchQuery(String docExpr, String ctsQuery, StringBuilder buf) {
        ctsQuery = ctsQuery.replaceAll("&", "&amp;");
        ctsQuery = ctsQuery.replaceAll("'", "&apos;");
        buf.append("fn:unordered(cts:search(");
        buf.append(docExpr);
        buf.append(",cts:query(xdmp:unquote('");
        buf.append(ctsQuery);
        buf.append("')/*),(\"unfiltered\",\"score-zero\")))");  
    }
    
    protected void buildSearchQuery(String docExpr, String ctsQuery, 
            Collection<String> nsCol, StringBuilder buf) {
        if (docExpr == null) {
            docExpr = "fn:collection()";
        }
        buf.append("xdmp:with-namespaces(("); 
        if (nsCol != null) {
            appendNamespace(nsCol, buf);
        }
        buf.append("),");      
        
        if (redactionRuleCol != null) {
            buf.append("rdt:redact(");
            buildSrcInSearchQuery(docExpr, ctsQuery, buf);
            buf.append("[$mlmr:splitstart to $mlmr:splitend]");
            buf.append(",((");
            
            for (int i = 0; i < redactionRuleCol.length; i++) {
                if (i != 0) {
                    buf.append(",");
                }
                buf.append("\"" + redactionRuleCol[i] + "\"");
            }
            buf.append(")))");
        } else {
            buildSrcInSearchQuery(docExpr, ctsQuery, buf);
            buf.append("[$mlmr:splitstart to $mlmr:splitend]");
        }
        
        buf.append(")");
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        mlSplit = (MarkLogicInputSplit)split;
        count = 0;
        context.getCounter(MarkLogicCounter.ESTIMATED_INPUT_RECORDS)
            .increment(mlSplit.getLength());

        // check hostnames
        hostNames = mlSplit.getLocations();
        if (hostNames == null || hostNames.length < 1) {
            throw new IllegalStateException("Empty split locations.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("split location: " + hostNames[0]);
        }

        replicas = mlSplit.getReplicas();
        curForest = -1;
        if (replicas != null) {
            for (int i = 0; i < replicas.size(); i++) {
                if (replicas.get(i).getHostName().equals(hostNames[0])) {
                   curForest = i;
                   break;
                }
            }
        }
        retry = 0;
        sleepTime = 500;
        init();
    }
        
    /* in case of failover, use init() instead of initialize() for retry */
    private void init() 
            throws IOException, InterruptedException {
        
        // get job config properties
        boolean advancedMode = 
            conf.get(INPUT_MODE, BASIC_MODE).equals(ADVANCED_MODE);
        redactionRuleCol = conf.getStrings(REDACTION_RULE_COLLECTION);
        
        // initialize the total length
        float recToFragRatio = conf.getFloat(RECORD_TO_FRAGMENT_RATIO, 
                getDefaultRatio());
        length = mlSplit.getLength() * recToFragRatio;
        
        // generate the query
        String queryLanguage = null;
        String queryText = null;
        long start = mlSplit.getStart() + 1 + count;
        long end = mlSplit.isLastSplit() ? 
                   Long.MAX_VALUE : start + mlSplit.getLength() - count - 1;
        if (!advancedMode) { 
            StringBuilder buf = new StringBuilder();      
            buf.append("xquery version \"1.0-ml\"; \n");
            if (redactionRuleCol != null) {
            	buf.append(
            		"import module namespace rdt = \"http://marklogic.com/xdmp/redaction\" at \"/MarkLogic/redaction.xqy\";\n");
            }
            String indent = conf.get(INDENTED, "FALSE");
            Indentation ind = Indentation.valueOf(indent);
            buf.append(
                    "declare namespace mlmr=\"http://marklogic.com/hadoop\";\n");
            buf.append(
                    "declare variable $mlmr:splitstart as xs:integer external;\n");
            buf.append(
                    "declare variable $mlmr:splitend as xs:integer external;\n");
           
            buf.append(ind.getStatement());
            buf.append("\n");
            String docExpr = conf.get(DOCUMENT_SELECTOR);
            String ctsQuery = conf.get(QUERY_FILTER);
            String subExpr = conf.get(SUBDOCUMENT_EXPRESSION);
            Collection<String> nsCol = docExpr != null || subExpr != null? 
                    conf.getStringCollection(PATH_NAMESPACE) : null;
            if (ctsQuery != null) {
                buildSearchQuery(docExpr, ctsQuery, nsCol, buf);
                queryText = buf.toString();
            } else if (docExpr != null || subExpr != null) {   
                buildDocExprQuery(docExpr, nsCol, subExpr, buf);
                queryText = buf.toString();
            } else {
                LexiconFunction function = null;
                Class<? extends LexiconFunction> lexiconClass = 
                        conf.getClass(INPUT_LEXICON_FUNCTION_CLASS, null,
                                LexiconFunction.class);
                if (lexiconClass != null) {
                    function = ReflectionUtils.newInstance(lexiconClass, 
                            conf);
                    if (nsCol == null) {
                        nsCol = conf.getStringCollection(PATH_NAMESPACE);
                    }
                    queryText = function.getInputQuery(nsCol, start, 
                            mlSplit.isLastSplit() ? 
                                    Long.MAX_VALUE : (long)length);
                }
            } 
            if (queryText == null) {
                buildDocExprQuery(docExpr, nsCol, subExpr, buf);
                queryText = buf.toString();
            }
        } else {
            queryText = conf.get(MarkLogicConstants.INPUT_QUERY);
            queryLanguage = conf.get(MarkLogicConstants.INPUT_QUERY_LANGUAGE);
            if (queryText == null) {
                throw new IllegalStateException(
                  "Input query is required in advanced mode but not defined.");
            }
        }
        
        if (LOG.isDebugEnabled()) {
            LOG.debug(queryText);
        }


        // set up a connection to the server
        while (retry < maxRetries) {
        try {
            if (retry == 1) {
                LOG.info("Retrying connect");
            }
            String curForestName = "";
            String curHostName = "";
            if (curForest == -1) {
                curForestName = mlSplit.getForestId().toString();
                curHostName = hostNames[0];
            } else {
                curForestName = replicas.get(curForest).getForest();
                curHostName = replicas.get(curForest).getHostName();
            }
            ContentSource cs = InternalUtilities.getInputContentSource(conf, 
                    curHostName);
            session = cs.newSession("#"+curForestName);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Connect to forest "
                    + curForestName + " on "
                    + session.getConnectionUri().getHost());
            }
            AdhocQuery query = session.newAdhocQuery(queryText);
            if (advancedMode) {
                boolean bindSplitRange = 
                        conf.getBoolean(BIND_SPLIT_RANGE, false);
                if (bindSplitRange) {
                    query.setNewIntegerVariable(MR_NAMESPACE, 
                            SPLIT_START_VARNAME, start);
                    query.setNewIntegerVariable(MR_NAMESPACE, 
                            SPLIT_END_VARNAME, end);
                } else {
                    query.setPosition(start);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("split start position: " + start);
                    }        
                    query.setCount(mlSplit.isLastSplit() ?
                            Long.MAX_VALUE : mlSplit.getLength());
                }
            } else {
                query.setNewIntegerVariable(MR_NAMESPACE, 
                        SPLIT_START_VARNAME, start);
                query.setNewIntegerVariable(MR_NAMESPACE, 
                        SPLIT_END_VARNAME, end);
            }
            RequestOptions options = new RequestOptions();
            options.setCacheResult(false);
            if (queryLanguage != null) {
            	options.setQueryLanguage(queryLanguage);
            }
            String ts = conf.get(INPUT_QUERY_TIMESTAMP);
            if (ts != null) {
                options.setEffectivePointInTime(new BigInteger(ts));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Query timestamp: " + ts);
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Input query: " + query.getQuery());
            }
            query.setOptions(options);       
            result = session.submitRequest(query);
        } catch (XccConfigException e) {
            LOG.error("XccConfigException:" + e);
            throw new IOException(e);
        } catch (QueryException e) {
            LOG.error("QueryException:" + e);
            throw new IOException(e);
        } catch (RequestException e) {
            boolean isRetryable = true;
            if (e instanceof MLCloudRequestException){
                isRetryable = ((MLCloudRequestException)e).isRetryable();
                LOG.error("MLCloudRequestException:" + e.getMessage());
            } else {
                LOG.error("RequestException:" + e.getMessage());
            }
            if (isRetryable && curForest != -1) {
                if (++retry < maxRetries) {
                    // failover
                    try {
                        InternalUtilities.sleep(sleepTime);
                    } catch (Exception e2) {
                    }
                    sleepTime = Math.min(sleepTime * 2,maxSleepTime);

                    curForest = (curForest+1)%replicas.size();
                    continue;
                 }
                 LOG.info("Exceeded max retry");
            }
            LOG.info("Query: " + queryText);
            if (LOG.isDebugEnabled()) {
                LOG.debug("RequestException: " + e);
            }
            throw new IOException(e);
        }
        break;
        } // retry
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        retry = 0;
        sleepTime = 500;
        while (retry < maxRetries) {
            try {
                if (result != null && result.hasNext()) {
                    ResultItem item = result.next();
                    boolean ret = nextResult(item);
                    // need to increment the count after nextResult is successful
                    count++;
                    return ret;
                } else {
                    endOfResult();
                    return false;
                }
            } catch (RuntimeException e) {
                LOG.error("RuntimeException:" + e.getMessage());
                if (curForest != -1) {
                    if (++retry < maxRetries) {
                        try {
                            InternalUtilities.sleep(sleepTime);
                        } catch (Exception e2) {
                        }
                        sleepTime = Math.min(sleepTime * 2,maxSleepTime);
    
                        curForest = (curForest+1)%replicas.size();
                        init();
                        continue;
                    }
                    LOG.info("Exceeded max retry");
                }
                throw e;
            }
        }
        endOfResult();
        return false;
    }

    abstract protected void endOfResult();

    abstract protected boolean nextResult(ResultItem result);
    
    protected abstract float getDefaultRatio();

    public long getCount() {
        return count;
    }
}

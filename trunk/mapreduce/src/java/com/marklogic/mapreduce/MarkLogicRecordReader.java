/*
 * Copyright 2003-2015 MarkLogic Corporation
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
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.mapreduce.functions.LexiconFunction;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
/**
 * A RecordReader that fetches data from MarkLogic server and generates 
 * <K, V> key value pairs.
 * 
 * @author jchen
 * 
 * @param <KEYIN, VALUEIN>
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
    
    private void buildDocExprQuery(String docExpr, Collection<String> nsCol, 
            StringBuilder buf) {
        String subExpr = conf.get(SUBDOCUMENT_EXPRESSION, "");
        String indent = conf.get(INDENTED, "FALSE");
        Indentation ind = Indentation.valueOf(indent);
        buf.append(
                "declare namespace mlmr=\"http://marklogic.com/hadoop\";\n");
        buf.append(
                "declare variable $mlmr:splitstart as xs:integer external;\n");
        buf.append(
                "declare variable $mlmr:splitend as xs:integer external;\n");
       
        buf.append(ind.getStatement());
        buf.append("xdmp:with-namespaces(("); 
        if (nsCol != null) {
            appendNamespace(nsCol, buf);
        }
        buf.append("),fn:unordered(fn:unordered(");
        buf.append(docExpr);
        buf.append(")[$mlmr:splitstart to $mlmr:splitend]");
        buf.append(subExpr);
        buf.append("))");
    }
    
    private void buildSearchQuery(String ctsQuery, Collection<String> nsCol,
            StringBuilder buf) {
        String indent = conf.get(INDENTED, "FALSE");
        Indentation ind = Indentation.valueOf(indent);
        buf.append(
                "declare namespace mlmr=\"http://marklogic.com/hadoop\";\n");
        buf.append(
                "declare variable $mlmr:splitstart as xs:integer external;\n");
        buf.append(
                "declare variable $mlmr:splitend as xs:integer external;\n");
       
        buf.append(ind.getStatement());
        buf.append("cts:search(fn:collection(),cts:query(xdmp:unquote('");
        buf.append(ctsQuery);
        buf.append("')/*),(\"unfiltered\",\"score-zero\",cts:unordered()))");
        buf.append("[$mlmr:splitstart to $mlmr:splitend]");
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        mlSplit = (MarkLogicInputSplit)split;
        count = 0;
        
        // check hostnames
        String[] hostNames = mlSplit.getLocations();
        if (hostNames == null || hostNames.length < 1) {
            throw new IllegalStateException("Empty split locations.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("split location: " + hostNames[0]);
        }
        
        // get job config properties
        boolean advancedMode = 
            conf.get(INPUT_MODE, BASIC_MODE).equals(ADVANCED_MODE);
        
        // initialize the total length
        float recToFragRatio = conf.getFloat(RECORD_TO_FRAGMENT_RATIO, 
                getDefaultRatio());
        length = mlSplit.getLength() * recToFragRatio;
        
        // generate the query
        String queryLanguage = null;
        String queryText = null;
        long start = mlSplit.getStart() + 1;
        long end = mlSplit.isLastSplit() ? 
                   Long.MAX_VALUE : start + mlSplit.getLength() - 1;
        if (!advancedMode) { 
            StringBuilder buf = new StringBuilder();      
            buf.append("xquery version \"1.0-ml\"; \n");
            String docExpr = conf.get(DOCUMENT_SELECTOR);
            Collection<String> nsCol = 
                    conf.getStringCollection(PATH_NAMESPACE);
            if (docExpr == null) {
                String ctsQuery = conf.get(QUERY_FILTER);
                if (ctsQuery != null) {
                    buildSearchQuery(ctsQuery, nsCol, buf);
                    queryText = buf.toString();
                } else {
                    LexiconFunction function = null;
                    Class<? extends LexiconFunction> lexiconClass = 
                        conf.getClass(INPUT_LEXICON_FUNCTION_CLASS, null,
                                LexiconFunction.class);
                    if (lexiconClass != null) {
                        function = ReflectionUtils.newInstance(lexiconClass, 
                                conf);
                        queryText = function.getInputQuery(nsCol, start, 
                                mlSplit.isLastSplit() ? 
                                        Long.MAX_VALUE : (long)length);
                    }
                }
            } 
            if (queryText == null) {
                if (docExpr == null) {
                    docExpr = "fn:collection()";
                }
                buildDocExprQuery(docExpr, nsCol, buf);
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
        try {
            ContentSource cs = InternalUtilities.getInputContentSource(conf, 
                    hostNames[0]);
            session = cs.newSession("#"+mlSplit.getForestId().toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Connect to forest "
                    + mlSplit.getForestId().toString() + " on "
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
            query.setOptions(options);       
            result = session.submitRequest(query);
        } catch (XccConfigException e) {
            LOG.error(e);
            throw new IOException(e);
        } catch (RequestException e) {
            LOG.error("Query: " + queryText);
            LOG.error(e);
            throw new IOException(e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (result != null && result.hasNext()) {
            ResultItem item = result.next();
            count++;
            return nextResult(item);
        } else {
            endOfResult();
            return false;
        }
    }

    abstract protected void endOfResult();

    abstract protected boolean nextResult(ResultItem result);
    
    protected abstract float getDefaultRatio();

    public long getCount() {
        return count;
    }
}

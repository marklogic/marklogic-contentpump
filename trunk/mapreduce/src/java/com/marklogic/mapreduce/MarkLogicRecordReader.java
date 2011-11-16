/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
    private MarkLogicInputSplit mlSplit;
    /**
     * Count of records fetched
     */
    protected long count;
    /**
     * Session to the MarkLogic server.
     */
    private Session session;
    /**
     * ResultSequence from the MarkLogic server.
     */
    protected ResultSequence result;
    /**
     * Job configuration.
     */
    private Configuration conf;
    /**
     * Total expected count of the records in a split.
     */
    private float length;
    
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

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        mlSplit = (MarkLogicInputSplit)split;
        count = 0;
        
        // construct the server URI
        URI serverUri;
        try {
            String[] hostNames = mlSplit.getLocations();
            if (hostNames == null || hostNames.length < 1) {
                throw new IllegalStateException("Empty split locations.");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("split location: " + hostNames[0]);
            }
            serverUri = InternalUtilities.getInputServerUri(conf, hostNames[0]);
        } catch (URISyntaxException e) {
            LOG.error(e);
            throw new IOException(e);
        } 
        
        // get job config properties
        boolean advancedMode = 
            conf.get(INPUT_MODE, BASIC_MODE).equals(ADVANCED_MODE);
        boolean bindSplitRange =conf.getBoolean(BIND_SPLIT_RANGE, false);
        
        // initialize the total length
        float recToFragRatio = conf.getFloat(RECORD_TO_FRAGMENT_RATIO, 
                getDefaultRatio());
        length = mlSplit.getLength() * recToFragRatio;
        
        // generate the query
        String queryText;
        long start = mlSplit.getStart() + 1;
        long end = mlSplit.isLastSplit() ? 
                   Long.MAX_VALUE : start + mlSplit.getLength() - 1;
        if (!advancedMode) {         
            LexiconFunction function = null;
            Class<? extends LexiconFunction> lexiconClass = 
                conf.getClass(INPUT_LEXICON_FUNCTION_CLASS, null,
                        LexiconFunction.class);
            Collection<String> nsCol = 
                conf.getStringCollection(PATH_NAMESPACE);
            if (lexiconClass != null) {
                function = ReflectionUtils.newInstance(lexiconClass, conf);
                queryText = function.getInputQuery(nsCol, start, 
                        mlSplit.isLastSplit() ? 
                                Long.MAX_VALUE : mlSplit.getLength());
            } else {
                String docExpr = conf.get(DOCUMENT_SELECTOR, 
                        MarkLogicInputFormat.DEFAULT_DOCUMENT_SELECTOR);
                String subExpr = conf.get(SUBDOCUMENT_EXPRESSION, "");
                StringBuilder buf = new StringBuilder();      
                buf.append("xquery version \"1.0-ml\"; \n");
                buf.append("xdmp:with-namespaces(("); 
                if (nsCol != null) {
                    for (Iterator<String> nsIt = nsCol.iterator(); 
                         nsIt.hasNext();) {
                        String ns = nsIt.next();
                        buf.append('"').append(ns).append('"');
                        if (nsIt.hasNext()) {
                            buf.append(',');
                        }
                    }
                }
                buf.append("),fn:unordered(fn:unordered(");
                buf.append(docExpr);
                buf.append(")[");
                buf.append(Long.toString(start));
                buf.append(" to ");
                buf.append(Long.toString(end));
                buf.append("]");
                buf.append(subExpr);
                buf.append("))");
                queryText = buf.toString();
            }
        } else {
            queryText = conf.get(MarkLogicConstants.INPUT_QUERY);
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
                    serverUri);
            session = cs.newSession("#"+mlSplit.getForestId().toString());
            AdhocQuery query = session.newAdhocQuery(queryText);
            if (advancedMode) {
                if (bindSplitRange) {
                    query.setNewIntegerVariable(SPLIT_RANGE_NAMESPACE, 
                            SPLIT_START_VARNAME, start);
                    query.setNewIntegerVariable(SPLIT_RANGE_NAMESPACE, 
                            SPLIT_END_VARNAME, end);
                } else {
                    query.setPosition(start);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("split start position: " + start);
                    }
                    
                    query.setCount(mlSplit.isLastSplit() ?
                            Long.MAX_VALUE : mlSplit.getLength());
                }
            }
            RequestOptions options = new RequestOptions();
            options.setCacheResult(false);
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

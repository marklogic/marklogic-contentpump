/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
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
import com.marklogic.xcc.types.XSInteger;
import com.marklogic.xcc.types.XSString;

/**
 * MarkLogic-based InputFormat superclass, taking a generic key and value 
 * class. Use the provided subclasses to configure your job, such as 
 * {@link DocumentInputFormat}.
 * 
 * @author jchen
 */
public abstract class MarkLogicInputFormat<KEYIN, VALUEIN> 
extends InputFormat<KEYIN, VALUEIN> implements MarkLogicConstants {
    public static final Log LOG =
        LogFactory.getLog(MarkLogicInputFormat.class);
    static final String DEFAULT_DOCUMENT_SELECTOR = "fn:collection()";
    static final String DEFAULT_CTS_QUERY = "cts:and-query(())";

    /**
     * Get input splits.
     * @param jobContext job context
     * @return list of input splits    
     */
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
            InterruptedException { 
        InternalUtilities.checkVersion();
        
        // get input from job configuration
        Configuration jobConf = jobContext.getConfiguration();
        long maxSplitSize;

        String docSelector;
        String splitQuery;
        LexiconFunction function = null;
        Class<? extends LexiconFunction> lexiconClass = 
            jobConf.getClass(INPUT_LEXICON_FUNCTION_CLASS, null,
                    LexiconFunction.class);
        if (lexiconClass != null) {
            // ignore setting for DOCUMENT_SELECTOR
            docSelector = DEFAULT_DOCUMENT_SELECTOR;
            function = ReflectionUtils.newInstance(lexiconClass, jobConf);
        } else {
            docSelector = jobConf.get(DOCUMENT_SELECTOR, 
                    DEFAULT_DOCUMENT_SELECTOR);
        }
        maxSplitSize = jobConf.getLong(MAX_SPLIT_SIZE, 
                DEFAULT_MAX_SPLIT_SIZE);
        if (maxSplitSize <= 0) {
            throw new IllegalStateException(
                "Max split size is required to be positive. It is set to " +
                maxSplitSize);
        }
        splitQuery = jobConf.get(SPLIT_QUERY, "");
        boolean advancedMode = 
            jobConf.get(INPUT_MODE, BASIC_MODE).equals(ADVANCED_MODE);
        
        // fetch data from server
        List<ForestSplit> forestSplits = null;
        Session session = null;
        ResultSequence result = null;
        
        if (!advancedMode) {
            Collection<String> nsCol = 
                jobConf.getStringCollection(PATH_NAMESPACE);        
            
            StringBuilder buf = new StringBuilder();
            buf.append("xquery version \"1.0-ml\";\n");
            buf.append("import module namespace hadoop = ");
            buf.append("\"http://marklogic.com/xdmp/hadoop\" at ");
            buf.append("\"/MarkLogic/hadoop.xqy\";\n");
            buf.append("hadoop:get-splits(");  
            
            if (nsCol != null && !nsCol.isEmpty()) {
                boolean isAlias = true;
                buf.append('\'');
                for (Iterator<String> nsIt = nsCol.iterator(); nsIt.hasNext();) {
                    String ns = nsIt.next();
                    if (isAlias) {
                        buf.append("declare namespace ");
                        buf.append(ns);
                        buf.append("=\"");
                        isAlias = false;
                    } else {
                        buf.append(ns);
                        buf.append("\";");
                        isAlias = true;
                    }
                    if (nsIt.hasNext() && isAlias) {
                        buf.append('\n');
                    }
                }
                buf.append("\', \'");
            } else {
                buf.append("'', \'");
            }
            
            buf.append(docSelector);
            buf.append("\', \'");
            if (function != null) {
                buf.append(function.getLexiconQuery());
            } else {
                buf.append(DEFAULT_CTS_QUERY);
            }
            buf.append("\')");
            
            splitQuery = buf.toString();
        } else {
            if (splitQuery.isEmpty()) {
                throw new IllegalStateException(
                  "Split query is required in advanced mode but not defined.");
            }
        }
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Split query: " + splitQuery);
        }
        try {
            ContentSource cs = InternalUtilities.getInputContentSource(jobConf);
            session = cs.newSession();
            AdhocQuery query = session.newAdhocQuery(splitQuery);
            RequestOptions options = new RequestOptions();
            options.setCacheResult(false);
            query.setOptions(options);
            result = session.submitRequest(query);
            
            int count = 0;
            while (result.hasNext()) {
                ResultItem item = result.next();
                if (forestSplits == null) {
                    forestSplits = new ArrayList<ForestSplit>();
                }
                int index = count % 3;
                if (index == 0) {
                    ForestSplit split = new ForestSplit();
                    split.forestId = ((XSInteger)item.getItem()).asBigInteger();
                    forestSplits.add(split);
                } else if (index == 1) {
                    forestSplits.get(forestSplits.size() - 1).recordCount = 
                        ((XSInteger)item.getItem()).asLong();
                } else if (index == 2) {
                    forestSplits.get(forestSplits.size() - 1).hostName = 
                        ((XSString)item.getItem()).asString();
                }
                count++;
            }
            LOG.info("Fetched " + forestSplits.size() + " forest splits.");
        } catch (XccConfigException e) {
            LOG.error(e);
            throw new IOException(e);
        } catch (RequestException e) {
            LOG.error(e);
            LOG.error("Query: " + splitQuery);
            throw new IOException(e);
        } catch (URISyntaxException e) {
            LOG.error(e);
            throw new IOException(e);
        } finally {
            if (result != null) {
                result.close();
            }
            if (session != null) {
                session.close();
            }
        }
        
        // construct and return splits
        List<InputSplit> splits = new ArrayList<InputSplit>();
        if (forestSplits == null || forestSplits.isEmpty()) {
            return splits;
        }
        
        for (ForestSplit fsplit : forestSplits) {
            if (fsplit.recordCount < maxSplitSize) {
                // assign length to max value when it is the last split,
                // since the record count is not accurate
                MarkLogicInputSplit split = 
                    new MarkLogicInputSplit(0, Long.MAX_VALUE, 
                            fsplit.forestId, fsplit.hostName);
                splits.add(split);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Added split " + split);
                }    
            } else {
                long splitCount = fsplit.recordCount / maxSplitSize;
                long remainder = fsplit.recordCount % maxSplitSize;
                if (remainder != 0) {
                    splitCount++;
                }
                long splitSize = fsplit.recordCount / splitCount;
                remainder = fsplit.recordCount % splitCount;
                if (remainder != 0) {
                    splitSize++;
                }
                long remainingCount = fsplit.recordCount;
                while (remainingCount > 0) {
                    long start = fsplit.recordCount - remainingCount;
                    // assign length to max value when it is the last split,
                    // since the record count is not accurate
                    long length = remainingCount <= maxSplitSize ? 
                                     Long.MAX_VALUE : splitSize; 
                    MarkLogicInputSplit split = 
                        new MarkLogicInputSplit(start, length, fsplit.forestId,
                                fsplit.hostName);
                    splits.add(split);
                    remainingCount -= length;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Added split " + split);
                    }
                }
            }
        }
        LOG.info("Made " + splits.size() + " splits.");
        return splits;
    }
    
    /**
     * A per-forest split of the result records.
     * 
     * @author jchen
     */
    class ForestSplit {
        BigInteger forestId;
        String hostName;
        long recordCount;
    }
}

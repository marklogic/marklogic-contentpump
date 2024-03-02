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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.marklogic.mapreduce.functions.LexiconFunction;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.mapreduce.utilities.RestrictedHostsUtil;
import com.marklogic.mapreduce.utilities.ForestHost;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.ItemType;
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
    static final String DEFAULT_CTS_QUERY = "()";
    Configuration jobConf = null;
    String docSelector;
    String localHost = null;
    boolean localMode = false;
    
    private void appendNsBindings(StringBuilder buf) {
        Collection<String> nsCol = 
                jobConf.getStringCollection(PATH_NAMESPACE);        
            
        if (nsCol != null && !nsCol.isEmpty()) {
            boolean isAlias = true;    
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
        }
    }
    
    private void appendDocumentSelector(StringBuilder buf) { 
        docSelector = jobConf.get(DOCUMENT_SELECTOR);
        if (docSelector != null) {
            buf.append(docSelector);
        } else {
            buf.append(DEFAULT_DOCUMENT_SELECTOR);
        }
    }
    
    protected void appendQuery(StringBuilder buf) {
        String ctsQuery = jobConf.get(QUERY_FILTER);
        if (ctsQuery != null) {
            ctsQuery = ctsQuery.replaceAll("&", "&amp;");
            ctsQuery = ctsQuery.replaceAll("'", "&apos;");
            ctsQuery = ctsQuery.replaceAll("&", "&amp;");
            ctsQuery = ctsQuery.replaceAll("\"", "&quot;");          
            buf.append("\"cts:query(xdmp:unquote('");
            buf.append(ctsQuery);
            buf.append("')/*)\"");
        } else if (docSelector != null) {
            buf.append("'");
            buf.append(DEFAULT_CTS_QUERY); 
            buf.append("'");
        } else {
            Class<? extends LexiconFunction> lexiconClass = 
                    jobConf.getClass(INPUT_LEXICON_FUNCTION_CLASS, null,
                            LexiconFunction.class);
            if (lexiconClass != null) {
                LexiconFunction function = 
                        ReflectionUtils.newInstance(lexiconClass, jobConf);
                buf.append("'");
                buf.append(function.getLexiconQuery());
                buf.append("'");
            } else {
                buf.append("'");
                buf.append(DEFAULT_CTS_QUERY);
                buf.append("'");
            }
        }
    }
    
    private void appendRedactionRuleValidateQuery(StringBuilder buf, String[] redactionRuleCol) {
        buf.append("\"REDACT\"");
        if (redactionRuleCol != null) {
            buf.append(",\n");
            buf.append("rdt:rule-validate((");
            for (int i = 0; i < redactionRuleCol.length; i++) {
                if (i != 0) {
                    buf.append(", ");
                }
                buf.append("\"");
                buf.append(redactionRuleCol[i]);
                buf.append("\"");
            }
            buf.append("))");
        }        
    }

    private void appendReplicaQuery(StringBuilder buf) {
        buf.append("let $repf := fn:function-lookup(");
        buf.append("xs:QName('hadoop:get-splits-with-replica'),0)\n");
        buf.append("return if (exists($repf)) then $repf() else ()\n");
    }
    
    protected void getForestSplits(JobContext jobContext,            
            ResultSequence result, 
            List<ForestSplit> forestSplits,
            List<String> ruleUris,
            String[] inputHosts,
            boolean getReplica) throws IOException {
        int count = 0;
        boolean restrictHosts = jobConf.getBoolean(INPUT_RESTRICT_HOSTS, false);
        RestrictedHostsUtil rhUtil = restrictHosts?new RestrictedHostsUtil(inputHosts):null;

        // First while loop: splits info
        while (result.hasNext()) {
            ResultItem item = result.next();
            int index = count % 3;
            if (index == 0) {
                ForestSplit split = new ForestSplit();
                if (ItemType.XS_STRING == item.getItemType()) {
                    if ("REDACT".equals(((XSString)item.getItem()).asString())) {
                        // Handled by next while loop
                        break;
                    } else {
                        throw new IOException("Unexpected string item from getSplits query result");
                    }
                }
                split.forestId = ((XSInteger)item.getItem()).asBigInteger();
                forestSplits.add(split);
            } else if (index == 1) {
                forestSplits.get(forestSplits.size() - 1).recordCount = 
                    ((XSInteger)item.getItem()).asLong();
            } else if (index == 2) {
                String forestHost = ((XSString) item.getItem()).asString();
                if (restrictHosts) {
                    rhUtil.addForestHost(forestHost);
                    forestSplits.get(forestSplits.size() - 1).hostName = forestHost;
                } else {
                    if (localMode && forestHost.equals(localHost)) {
                        forestSplits.get(forestSplits.size() - 1).hostName = inputHosts[0];
                    } else {
                        forestSplits.get(forestSplits.size() - 1).hostName = forestHost;
                    }
                }
                
            }
            count++;
        }
        // Replace the target host if not in restricted host list
        if (restrictHosts) {
            for (ForestSplit split : forestSplits) {
                split.hostName = rhUtil.getNextHost(split.hostName);
            }
        }
        
        // Second while loop: redaction rules
        while (result.hasNext()) {
            ResultItem item = result.next();
            if (ItemType.XS_INTEGER == item.getItemType()) {
                if (((XSInteger)item.getItem()).asPrimitiveInt() == 0) {
                    break;
                } else {
                    throw new IOException("Unexpected item " + item.getItemType().toString());
                }
            } else if (ItemType.XS_STRING != item.getItemType() ) {
                throw new IOException("Unexpected item " + item.getItemType().toString());
            }
            String itemStr = ((XSString)item.getItem()).asString();
            ruleUris.add(itemStr);
        }

        // forest with failover hosts
        if (getReplica) {
            String forest = "";
            String hostName = "";
            HashMap<String, List<ForestHost> > 
                forestHostMap = new HashMap<>();
            while (result.hasNext()) {
                ResultItem item = result.next();
                if (ItemType.XS_INTEGER == item.getItemType()) {
                    if (((XSInteger)item.getItem()).asPrimitiveInt() == 0) {
                        break;
                    }
                }
                forest = item.asString();

                if (result.hasNext()) {
                    item = result.next();
                    hostName = item.asString();

                    List<ForestHost> replicas = new ArrayList<>();
                    String replicaForest = "";
                    String replicaHost = "";
                    while (result.hasNext()) {
                        item = result.next();
                        if (ItemType.XS_INTEGER == item.getItemType()) {
                            if (((XSInteger)item.getItem()).asPrimitiveInt() == 0) {
                                break;
                            }
                        }
                        replicaForest = item.asString();
                        if (result.hasNext()) {
                            item = result.next();
                            replicaHost = item.asString();
                            if (localMode && replicaHost.equals(localHost)) {
                                replicaHost = inputHosts[0];
                            }
                            ForestHost info = new ForestHost(replicaForest, replicaHost);
                            replicas.add(info);
                        }
                    }
                    forestHostMap.put(forest,replicas);
                }
            } // while (result.hasNext())
            if (!restrictHosts) {
                for (ForestSplit split : forestSplits) {
                    split.replicas = forestHostMap.get(split.forestId.toString());
                }
            }
        } // if (getReplica)
    }
    
    protected void appendCustom(StringBuilder buf) {
        buf.append("()");
    }
    
    /**
     * Get input splits.
     * @param jobContext job context
     * @return list of input splits    
     */
    @SuppressWarnings("unchecked")
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
            InterruptedException { 
        // get input from job configuration
        jobConf = jobContext.getConfiguration();        
        boolean advancedMode = 
                jobConf.get(INPUT_MODE, BASIC_MODE).equals(ADVANCED_MODE);
        String restrictHostsString = jobConf.getTrimmed(INPUT_RESTRICT_HOSTS);
        boolean restrictHosts = false;
        if (restrictHostsString!=null && !restrictHostsString.isEmpty()) {
            restrictHosts = Boolean.parseBoolean(restrictHostsString);
        }
        boolean getForwardHeader = restrictHostsString == null;
        boolean getReplica = !restrictHosts;
        String splitQuery;
        String queryLanguage = null;
        String[] redactionRuleCol = jobConf.getStrings(REDACTION_RULE_COLLECTION);
        String[] inputHosts = jobConf.getStrings(INPUT_HOST);
        if (inputHosts == null || inputHosts.length == 0) {
            throw new IllegalStateException(INPUT_HOST + " is not specified.");
        }
        
        if (advancedMode) {
            queryLanguage = jobConf.get(INPUT_QUERY_LANGUAGE);
            splitQuery = jobConf.get(SPLIT_QUERY);
        } else {
            StringBuilder buf = new StringBuilder();
            buf.append("xquery version \"1.0-ml\";\n");
            if (getForwardHeader) {
                buf.append("fn:exists(xdmp:get-request-header('x-forwarded-for'));\n");
            }
            buf.append("import module namespace hadoop = ");
            buf.append("\"http://marklogic.com/xdmp/hadoop\" at ");
            buf.append("\"/MarkLogic/hadoop.xqy\";\n");
            if (redactionRuleCol != null) {
                buf.append("import module namespace rdt = "
                        + "\"http://marklogic.com/xdmp/redaction\" at "
                        + "\"/MarkLogic/redaction.xqy\";\n");
            }
            buf.append("xdmp:host-name(xdmp:host()),\n");            
            buf.append("hadoop:get-splits(\'"); 
            appendNsBindings(buf);
            buf.append("\', \'");
            appendDocumentSelector(buf);
            buf.append("\',");
            appendQuery(buf);
            buf.append("),\n");
            appendRedactionRuleValidateQuery(buf, redactionRuleCol);
            buf.append(",0,");
            if (!restrictHosts) {
                appendReplicaQuery(buf);
                buf.append(",0,");
            }
            appendCustom(buf);
            splitQuery = buf.toString();
        } 
        
        String mode = jobConf.get(EXECUTION_MODE,MODE_DISTRIBUTED);
        long defaultSplitSize = mode.equals(MODE_DISTRIBUTED) ? 
            DEFAULT_MAX_SPLIT_SIZE : DEFAULT_LOCAL_MAX_SPLIT_SIZE;
        long maxSplitSize = jobConf.getLong(MAX_SPLIT_SIZE, defaultSplitSize);
        if (maxSplitSize <= 0) {
            throw new IllegalStateException(
                "Max split size is required to be positive. It is set to " +
                maxSplitSize);
        }

        // fetch data from server
        List<ForestSplit> forestSplits = new ArrayList<>();
        Session session = null;
        ResultSequence result = null;            
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Split query: " + splitQuery);
        }
        localMode = MODE_LOCAL.equals(jobConf.get(EXECUTION_MODE));

        int hostIdx = 0;
        while (hostIdx < inputHosts.length) {
            try {
                ContentSource cs = InternalUtilities.getInputContentSource(jobConf,
                        inputHosts[hostIdx]);
                session = cs.newSession();
                RequestOptions options = new RequestOptions();
                options.setCacheResult(false);

                if (localMode && advancedMode) {
                    AdhocQuery hostQuery = session.newAdhocQuery(
                        "xquery version \"1.0-ml\";xdmp:host-name(xdmp:host())");
                    hostQuery.setOptions(options);
                    result = session.submitRequest(hostQuery);
                    if(result.hasNext()) {
                        ResultItem item = result.next();
                        localHost = item.asString();
                    }
                    if (result != null) {
                        result.close();
                    }
                }

                AdhocQuery query = session.newAdhocQuery(splitQuery);
                if (queryLanguage != null) {
                    InternalUtilities.checkQueryLanguage(queryLanguage);
                    options.setQueryLanguage(queryLanguage);
                }
                query.setOptions(options);
                result = session.submitRequest(query);
                boolean forwardHeaderExists = false;
                if (!advancedMode && result.hasNext()) {
                    ResultItem item = result.next();
                    if (getForwardHeader) {
                        forwardHeaderExists = item.asString().equals("true");
                        item = result.next();
                        if (forwardHeaderExists) {
                            restrictHosts = true;
                            jobConf.setBoolean(INPUT_RESTRICT_HOSTS, true);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("HTTP compliant mode enabled since x-forwarded-for exists");
                            }
                        } // delay disable http mode since the output host may need it
                    }
                    localHost = item.asString();
                }
                List<String> ruleUris = null;
                if (redactionRuleCol != null) {
                    ruleUris = new ArrayList<>();
                }
                getForestSplits(jobContext, result, forestSplits,
                        ruleUris, inputHosts, getReplica);
                LOG.info("Fetched " + forestSplits.size() +
                        " forest splits.");
                break;
            } catch (ServerConnectionException e) {
                LOG.warn("Unable to connect to " + inputHosts[hostIdx]
                        + " to query source information");
                hostIdx++;
            } catch (XccConfigException e) {
                LOG.error(e);
                throw new IOException(e);
            } catch (RequestException e) {
                LOG.error(e);
                LOG.error("Query: " + splitQuery);
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
        if (hostIdx == inputHosts.length) {
            // No usable input hostname found at this point
            throw new IOException("Unable to query source information,"
                    + " no usable hostname found");
        }
        
        // create a split list per forest per host
        if (forestSplits == null || forestSplits.isEmpty()) {
            return new ArrayList<>();
        }
        
        // construct a list of splits per forest per host
        Map<String, List<List<InputSplit>>> hostForestSplits =
            new HashMap<>();
        boolean tsQuery = (jobConf.get(INPUT_QUERY_TIMESTAMP) != null);
        for (ForestSplit fsplit : forestSplits) {
            List<InputSplit> splits = null;
            if (fsplit.recordCount > 0 || !tsQuery) {
                String host = fsplit.hostName;
                List<List<InputSplit>> splitLists = hostForestSplits.get(host);
                if (splitLists == null) {
                    splitLists = new ArrayList<>();
                    hostForestSplits.put(host, splitLists);
                }
                splits = new ArrayList<>();
                splitLists.add(splits);
            } else {
                continue;
            }

            long splitSize = maxSplitSize;
            if (this instanceof KeyValueInputFormat<?, ?> &&
                (splitSize & 0x1) != 0) {
                splitSize--;
            }
            long remainingCount = fsplit.recordCount;
            // split size zero or negative means unknown split size
            if (remainingCount <= 0) {
                MarkLogicInputSplit split = new MarkLogicInputSplit(0, 0,
                    fsplit.forestId, fsplit.hostName, fsplit.replicas);
                split.setLastSplit(true);
                splits.add(split);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Added split " + split);
                }
            } else {
                while (remainingCount > 0L) {
                    long start = fsplit.recordCount - remainingCount;
                    MarkLogicInputSplit split;
                    if (remainingCount < splitSize) {
                        split = new MarkLogicInputSplit(start, remainingCount,
                            fsplit.forestId, fsplit.hostName, fsplit.replicas);
                        split.setLastSplit(true);
                        remainingCount = 0L;
                    } else {
                        split = new MarkLogicInputSplit(start, splitSize,
                            fsplit.forestId, fsplit.hostName, fsplit.replicas);
                        remainingCount -= splitSize;
                    }
                    splits.add(split);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Added split " + split);
                    }
                }
            }
        }
        
        // mix the lists of splits into one per host
        Set<String> hosts = hostForestSplits.keySet();
        int hostCount = hosts.size();
        List<InputSplit>[] hostSplits = (List<InputSplit>[])
                new List<?>[hostCount];
        int i = 0;
        for (String host : hosts) {
            List<List<InputSplit>> splitLists = hostForestSplits.get(host);
            if (splitLists.size() == 1) {
                hostSplits[i++] = splitLists.get(0);
            } else {
                hostSplits[i] = new ArrayList<>();
                boolean more = true;
                for (int j = 0; more; j++) {
                    more = false;
                    for (List<InputSplit> splitsPerForest : splitLists) {
                        if (j < splitsPerForest.size()) {
                            hostSplits[i].add(splitsPerForest.get(j));
                        }
                        more = more || (j + 1 < splitsPerForest.size());
                    }
                }
                i++;
            }
        }
        
        // mix hostSplits into one
        List<InputSplit> splitList = new ArrayList<>();
        boolean more = true;
        for (int j = 0; more; j++) {
            more = false;
            for (List<InputSplit> splitsPerHost : hostSplits) {
                if (j < splitsPerHost.size()) {
                    splitList.add(splitsPerHost.get(j));
                }
                more = more || (j + 1 < splitsPerHost.size());
            }
        }        
        
        LOG.info("Made " + splitList.size() + " split(s).");
        if (LOG.isDebugEnabled()) {
            for (InputSplit split : splitList) {
                LOG.debug(split);
            }
        }
        return splitList;
    }

    /**
     * A per-forest split of the result records.
     * 
     * @author jchen
     */
    protected class ForestSplit {
        BigInteger forestId;
        String hostName;
        long recordCount;
        List<ForestHost> replicas;
    }

}

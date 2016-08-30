/*
 * Copyright 2003-2016 MarkLogic Corporation
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
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicDocument;
import com.marklogic.mapreduce.MarkLogicInputSplit;
import com.marklogic.mapreduce.MarkLogicRecordReader;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.mapreduce.utilities.URIUtil;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.JsonItem;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XSInteger;
import com.marklogic.xcc.types.XdmElement;
import com.marklogic.xcc.types.XdmItem;

/**
 * A MarkLogicRecordReader that fetches data from MarkLogic server and generates 
 * <DocumentURI, MarkLogicDocument> key value pairs.
 * 
 * @author ali
 * 
 */

//can't reuse MarkLogicRecordReader, because the prolog of the query need to be
//changed, can't simply change query body

public class DatabaseContentReader extends
    MarkLogicRecordReader<DocumentURI, MarkLogicDocument> implements
    ConfigConstants {
    static final float DOCUMENT_TO_FRAGMENT_RATIO = 1;
    public static final Log LOG = LogFactory
        .getLog(DatabaseContentReader.class);
    protected boolean copyCollection;
    protected boolean copyPermission;
    protected boolean copyProperties;
    protected boolean copyQuality;
    protected boolean copyMetadata;
    protected HashMap<String, DocumentMetadata> metadataMap;
    protected String ctsQuery = null;
    protected boolean nakedDone = false;
    /**
     * Current key.
     */
    protected DocumentURI currentKey;
    /**
     * Current value.
     */
    protected DatabaseDocumentWithMeta currentValue;

    public DatabaseContentReader(Configuration conf) {
        super(conf);
        copyCollection = conf.getBoolean(MarkLogicConstants.COPY_COLLECTIONS, 
                true);
        copyPermission = conf.getBoolean(CONF_COPY_PERMISSIONS, true);
        copyProperties = conf.getBoolean(CONF_COPY_PROPERTIES,true);
        copyQuality = conf.getBoolean(MarkLogicConstants.COPY_QUALITY, true);
        copyMetadata = conf.getBoolean(MarkLogicConstants.COPY_METADATA, true);
        currentKey = new DocumentURI();
        metadataMap = new HashMap<>();
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        mlSplit = (MarkLogicInputSplit) inSplit;
        count = 0;

        // construct the server URI
        String[] hostNames = mlSplit.getLocations();
        if (hostNames == null || hostNames.length < 1) {
            throw new IllegalStateException("Empty split locations.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("split location: " + hostNames[0]);
        }

        // initialize the total length
        float recToFragRatio = conf.getFloat(RECORD_TO_FRAGMENT_RATIO,
            getDefaultRatio());
        length = mlSplit.getLength() * recToFragRatio;

        // generate the query
        String queryText;
        long start = mlSplit.getStart() + 1;
        long end = mlSplit.isLastSplit() ? Long.MAX_VALUE : start
            + mlSplit.getLength() - 1;

        String src = conf.get(MarkLogicConstants.DOCUMENT_SELECTOR);
        redactionRuleCol = conf.getStrings(REDACTION_RULE_COLLECTION);
        Collection<String> nsCol = null;
        if (src != null) {
            nsCol = conf.getStringCollection(
                    MarkLogicConstants.PATH_NAMESPACE);
        } else {
            src = "fn:collection()";
        }
        ctsQuery = conf.get(MarkLogicConstants.QUERY_FILTER);
        StringBuilder buf = new StringBuilder();
        if (ctsQuery != null) {
            buildSearchQuery(src, ctsQuery, nsCol, buf);
        } else {
            buildDocExprQuery(src, nsCol, null, buf);
        }   
        src = buf.toString();
        buf = new StringBuilder();
        buf.append("xquery version \"1.0-ml\"; \n");
        buf.append("import module namespace hadoop = ");
        buf.append("\"http://marklogic.com/xdmp/hadoop\" at ");
        buf.append("\"/MarkLogic/hadoop.xqy\";\n");
        if (redactionRuleCol != null) {
            buf.append(
                "import module namespace rdt = \"http://marklogic.com/xdmp/redaction\" at \"/MarkLogic/redaction.xqy\";\n");
        }
        buf.append(
                "declare namespace mlmr=\"http://marklogic.com/hadoop\";\n");
        buf.append("declare option xdmp:output \"indent=no\";\n");
        buf.append("declare option xdmp:output \"indent-untyped=no\";\n");
        buf.append(
                "declare variable $mlmr:splitstart as xs:integer external;\n");
        buf.append(
                "declare variable $mlmr:splitend as xs:integer external;\n");
        buf.append("let $cols := ");
        buf.append(src);
        buf.append("\nlet $all-meta :=");
        buf.append("\nfor $doc in $cols");
        buf.append("\nlet $uri := fn:base-uri($doc)\n return (");

        buf.append("'META',");
        buf.append("$uri,");
        buf.append("if(fn:empty($doc/node())) then 0 else xdmp:node-kind($doc/node())");
        if (copyCollection || copyPermission || copyProperties || copyQuality) {
            buf.append(",");
            if (copyCollection) {
                buf.append("xdmp:document-get-collections($uri),\n");
            }
            if (copyPermission) {
                buf.append("let $list := xdmp:document-get-permissions($uri)\n");
                buf.append("return hadoop:get-permissions($list),");
            }
            // if copy-quality, else + 0
            if (copyQuality) {
                buf.append("xdmp:document-get-quality($uri),\n");
            } else {
                buf.append("0,");
            }
            // if copy-metadata
            if (copyMetadata) {
                buf.append(
                    "(let $f := fn:function-lookup(xs:QName('xdmp:document-get-metadata'),1)\n"
                    + "return if (exists($f)) then $f($uri) else ()),\n");
            }
            // if copy-properties, else + (),\n
            if (copyProperties) {
                buf.append("xdmp:document-properties($uri)/prop:properties,\n");
            }
        } else {
            buf.append(",0,"); // quality
            buf.append("(),\n");//properties
        }
        // end-of-record marker
        buf.append("0");       
        buf.append(" )\n");
        buf.append("return ($all-meta");
        buf.append(",'EOM',$cols)");

        queryText = buf.toString();
        if (LOG.isDebugEnabled()) {
            LOG.debug(queryText);
        }

        // set up a connection to the server
        try {
            ContentSource cs = InternalUtilities.getInputContentSource(conf,
                hostNames[0]);
            session = cs.newSession("#" + mlSplit.getForestId().toString());
            AdhocQuery aquery = session.newAdhocQuery(queryText);
            aquery.setNewIntegerVariable(MR_NAMESPACE, SPLIT_START_VARNAME,
                start);
            aquery.setNewIntegerVariable(MR_NAMESPACE, SPLIT_END_VARNAME, end);
            RequestOptions options = new RequestOptions();
            options.setCacheResult(false);
            String ts = conf.get(INPUT_QUERY_TIMESTAMP);
            if (ts != null) {
                options.setEffectivePointInTime(new BigInteger(ts));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Query timestamp: " + ts);
                }
            } 
            aquery.setOptions(options);
            result = session.submitRequest(aquery);
            
            initMetadataMap();
        } catch (XccConfigException e) {
            LOG.error(e);
            throw new IOException(e);
        } catch (RequestException e) {
            LOG.error("Query: " + queryText);
            LOG.error(e);
            throw new IOException(e);
        }
    }
     
    protected void queryNakedProperties() throws IOException {
        StringBuilder buf = new StringBuilder();
        buf.append("xquery version \"1.0-ml\"; \n");
        buf.append("import module namespace hadoop = ");
        buf.append("\"http://marklogic.com/xdmp/hadoop\" at ");
        buf.append("\"/MarkLogic/hadoop.xqy\";\n");
        buf.append("let $props := cts:search(");
        String cFilter = null, dFilter = null;
        cFilter = conf.get(MarkLogicConstants.COLLECTION_FILTER);
        if (cFilter != null) {
            buf.append("xdmp:collection-properties(");
            buf.append(cFilter);
            buf.append(")");
        } else {
            dFilter = conf.get(MarkLogicConstants.DIRECTORY_FILTER);
            if (dFilter != null) {
                buf.append("xdmp:directory-properties(");
                buf.append(dFilter);
                buf.append(", \"infinity\")");
            } else {
                buf.append("xdmp:collection-properties()");
            }
        }                
        buf.append(",");
        if (ctsQuery == null) {
            buf.append(
                    "cts:not-query(cts:document-fragment-query(");
            buf.append("cts:and-query(()))),");
            buf.append("(\"unfiltered\",\"score-zero\"))\n");
        } else {
            buf.append("cts:and-query((cts:query(xdmp:unquote('");
            buf.append(ctsQuery);
            buf.append("')/*),cts:not-query(cts:document-fragment-query(");
            buf.append("cts:and-query(()))))),");
            buf.append("(\"unfiltered\",\"score-zero\"))\n");
        }
        buf.append("for $doc in $props\n");
        buf.append("let $uri := fn:base-uri($doc)\n return (");

        buf.append("'META',");
        buf.append("$uri,");
        buf.append("if(fn:empty($doc/node())) then 0 else xdmp:node-kind($doc/node()),");
        if (copyCollection) {
            buf.append("xdmp:document-get-collections($uri),\n");
        }
        if (copyPermission) {
            buf.append("let $list := xdmp:document-get-permissions($uri)\n");
            buf.append("return hadoop:get-permissions($list),");
        }
        // if copy-quality, else + 0
        if (copyQuality) {
            buf.append("xdmp:document-get-quality($uri),\n");
        } else {
            buf.append("0,");
        }
        // copy-metadata
        if (copyMetadata) {
            buf.append("(let $f := fn:function-lookup(xs:QName('xdmp:document-get-metadata'),1)\n"
                    + "return if (exists($f)) then $f($uri) else ()),\n");
        }
        buf.append("$doc/prop:properties, \n");

        // end-of-record marker
        buf.append("0");

        buf.append(")");
        String queryText = buf.toString();
        if (LOG.isDebugEnabled()) {
            LOG.debug(queryText);
        }
        
        // set up a connection to the server
        try {
            AdhocQuery aquery = session.newAdhocQuery(queryText);
            RequestOptions options = new RequestOptions();
            options.setCacheResult(false);
            String ts = conf.get(INPUT_QUERY_TIMESTAMP);
            if (ts != null) {
                options.setEffectivePointInTime(new BigInteger(ts));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Query timestamp: " + ts);
                }
            } 
            aquery.setOptions(options);
            result = session.submitRequest(aquery);
            nakedDone = true;
        } catch (RequestException e) {
            LOG.error("Query: " + queryText);
            LOG.error(e);
            throw new IOException(e);
        }
    }

    private void initMetadataMap() throws IOException {
        while (result.hasNext()) {
            ResultItem item = result.next();
            String type = null;
            if (item != null && item.getItemType() == ValueType.XS_STRING) {
                type = item.asString();
            } else {
                throw new IOException("incorrect format:" + item.getItem()
                    + "\n" + result.asString());
            }

            if ("META".equals(type)) {
                DocumentMetadata metadata = new DocumentMetadata();
                String uri = parseMetadata(metadata);
                metadataMap.put(uri, metadata);
            } else if ("EOM".equals(type)) {
                //end of metadata
                return;
            } else {
                throw new IOException("incorrect type");
            }
        }
    }
    
    /**
     * Parse metadata from the sequence, store it into the DocumentMetadata
     * object passed in
     * 
     * @param metadata
     * @return uri of the document with this metadata
     * @throws IOException
     */

    private String parseMetadata(DocumentMetadata metadata) throws IOException {
        ResultItem item = result.next();
        String uri = item.asString();
        if (uri == null) {
            throw new IOException("Missing document URI for metadata.");
        }
        item = result.next();
        //node-kind, must exist
        String nKind = item.asString();
        metadata.setFormat(nKind);
        
        item = result.next();
        // handle collections, may not be present
        while (item != null && item.getItemType() == ValueType.XS_STRING) {
            if (!copyCollection) {
                item = result.next();
                continue;
            }
            metadata.addCollection(item.asString());
            item = result.next();
        }
        
        // handle permissions, may not be present
        StringBuilder buf = new StringBuilder();
        buf.append("<perms>");
        while (item != null && ValueType.ELEMENT == item.getItemType()) {
            if (!copyPermission) {
                item = result.next();
                continue;
            }
            try {
                readPermission((XdmElement) item.getItem(), metadata, buf);
            } catch (Exception e) {
                throw new IOException(e);
            }
            item = result.next();
        }
        buf.append("</perms>");
        metadata.setPermString(buf.toString());
        
        // handle quality, always present even if not requested (barrier)
        metadata.setQuality((XSInteger) item.getItem());
        
        // handle metadata
        item = result.next();
        if (copyMetadata) {
            XdmItem metaItem  = item.getItem();
            if (metaItem instanceof JsonItem) {
                JsonNode node = ((JsonItem)metaItem).asJsonNode();
                metadata.meta = new HashMap<>(node.size());
                for (Iterator<String> names = node.fieldNames(); 
                     names.hasNext();) {
                    String key = names.next();
                    JsonNode nodeVal = node.get(key);
                    metadata.meta.put(key, nodeVal.asText());
                }
                item = result.next();
            }
        }

        // handle prop:properties node, optional
        // if not present, there will be a 0 as a marker
    
        if (copyProperties && ValueType.ELEMENT == item.getItemType()) {
            String pString = item.asString();
            if (pString != null) {
                metadata.setProperties(pString);
            }
            item = result.next();
        }
        if (ValueType.XS_INTEGER != item.getItemType()) {
            throw new IOException(uri + " unexpected " + item.getItemType() + " "
                + item.asString() + ", expected " + ValueType.XS_INTEGER
                + " 0");
        }
        return uri;
    }
   
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (result == null || (!result.hasNext())) {
            if (!nakedDone && copyProperties && mlSplit.getStart() == 0) {
                queryNakedProperties();
                if (!result.hasNext()) {
                    return false;
                }
            } else {
                return false;
            }
        }
        ResultItem currItem = null;
        currItem = result.next();

        if (!nakedDone) {
            count++;
            // docs
            String uri = null;
            uri = currItem.getDocumentURI();
            if (uri == null) {
                throw new IOException("Missing document URI for result "
                    + currItem.toString());
            }
            currentValue = new DatabaseDocumentWithMeta();
            DocumentMetadata metadata = metadataMap.get(uri);
            uri = URIUtil.applyUriReplace(uri, conf);
            uri = URIUtil.applyPrefixSuffix(uri, conf);
            currentKey.setUri(uri);
            if (metadata != null) {
                currentValue.setMeta(metadata);
                currentValue.set(currItem);
            } else {
                LOG.error("no meta for " + uri);
            }
            return true;
        } else { // naked properties
            currentValue = new DatabaseDocumentWithMeta();
            ResultItem item = currItem;
            String type = null;
            if (item != null && item.getItemType() == ValueType.XS_STRING) {
                type = item.asString();
            } else {
                throw new IOException("incorrect format:" + item.getItem()
                    + "\n" + result.asString());
            }
            if ("META".equals(type)) {
                DocumentMetadata metadata = new DocumentMetadata();
                String uri = parseMetadata(metadata);
                metadata.setNakedProps(true);
                uri = URIUtil.applyUriReplace(uri, conf);
                uri = URIUtil.applyPrefixSuffix(uri, conf);
                currentKey.setUri(uri);
                currentValue.setMeta(metadata);
                currentValue.setContentType(ContentType.XML);
            } else {
                throw new IOException("incorrect type");
            }
        }
        return true;
    }

    @Override
    protected boolean nextResult(ResultItem result) {
        return false;
    }

    private void readPermission(XdmElement _permissionElement,
            DocumentMetadata metadata, StringBuilder buf) throws Exception {
        // permission: turn into a ContentPermission object
        // each permission is a sec:permission element.
        // children:
        // sec:capability ("read", "insert", "update")
        // and sec:role xs:unsignedLong (but we need string)
        String permString = _permissionElement.asString();
        int i = permString.indexOf("<sec:role-name>");
        int j = permString.indexOf("</sec:role-name>")+16;
        buf.append(permString.substring(0, i));
        buf.append(permString.substring(j));   
        Element permissionW3cElement = _permissionElement.asW3cElement();

        NodeList capabilities = permissionW3cElement
            .getElementsByTagName("sec:capability");
        NodeList roles = permissionW3cElement
            .getElementsByTagName("sec:role-name");
        Node role;
        Node capability;
        if (0 < roles.getLength() && 0 < capabilities.getLength()) {
            role = roles.item(0);
            capability = capabilities.item(0);
            metadata.addPermission(capability.getTextContent(),
                role.getTextContent());
            if (roles.getLength() > 1) {
                LOG.warn("input permission: " + permissionW3cElement + ": "
                    + roles.getLength() + " roles, using only 1");
            }
            if (capabilities.getLength() > 1) {
                LOG.warn("input permission: " + permissionW3cElement + ": "
                    + capabilities.getLength() + " capabilities, using only 1");
            }
        } else {
            // warn and skip
            if (roles.getLength() < 1) {
                LOG.warn("skipping input permission: " + permissionW3cElement
                    + ": no roles");
            }
            if (capabilities.getLength() < 1) {
                LOG.warn("skipping input permission: " + permissionW3cElement
                    + ": no capabilities");
            }
        }

    }

    @Override
    protected void endOfResult() {
        currentKey = null;
        currentValue = null;
    }

    @Override
    protected float getDefaultRatio() {
        return DOCUMENT_TO_FRAGMENT_RATIO;
    }

    @Override
    public DocumentURI getCurrentKey() throws IOException,
        InterruptedException {
        return currentKey;
    }

    @Override
    public MarkLogicDocument getCurrentValue() throws IOException,
        InterruptedException {
        return currentValue;
    }

}

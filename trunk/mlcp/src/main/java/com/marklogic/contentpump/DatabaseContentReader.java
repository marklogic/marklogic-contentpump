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

package com.marklogic.contentpump;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.marklogic.contentpump.utilities.URIUtil;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.MarkLogicDocument;
import com.marklogic.mapreduce.MarkLogicInputSplit;
import com.marklogic.mapreduce.MarkLogicRecordReader;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XSInteger;
import com.marklogic.xcc.types.XdmElement;

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
    MarkLogicRecordReader<DocumentURI, MarkLogicDocument> {
    static final float DOCUMENT_TO_FRAGMENT_RATIO = 1;
    public static final Log LOG = LogFactory
        .getLog(DatabaseContentReader.class);
    protected boolean copyCollection;
    protected boolean copyPermission;
    protected boolean copyProperties;
    protected boolean copyQuality;
    protected HashMap<String, DocumentMetadata> metadataMap;
    /**
     * Current key.
     */
    protected DocumentURI currentKey;
    /**
     * Current value.
     */
    protected MarkLogicDocumentWithMeta currentValue;

    public DatabaseContentReader(Configuration conf) {
        super(conf);
        copyCollection = conf.getBoolean(
            ConfigConstants.CONF_COPY_COLLECTIONS, false);
        copyPermission = conf.getBoolean(
            ConfigConstants.CONF_COPY_PERMISSIONS, false);
        copyProperties = conf.getBoolean(ConfigConstants.CONF_COPY_PROPERTIES,
            false);
        copyQuality = conf
            .getBoolean(ConfigConstants.CONF_COPY_QUALITY, false);
        currentKey = new DocumentURI();
        metadataMap = new HashMap<String, DocumentMetadata>();
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

        String src = conf.get(MarkLogicConstants.DOCUMENT_SELECTOR, "fn:collection()");
        String cFilter = conf.get(ConfigConstants.CONF_COLLECTION_FILTER);
        String dFilter = conf.get(ConfigConstants.CONF_DIRECTORY_FILTER);
        
        StringBuilder buf = new StringBuilder();
        buf.append("xquery version \"1.0-ml\"; \n");
        buf.append("import module namespace hadoop = ");
        buf.append("\"http://marklogic.com/xdmp/hadoop\" at ");
        buf.append("\"/MarkLogic/hadoop.xqy\";\n");
        buf.append("declare namespace mlmr=\"http://marklogic.com/hadoop\";");
        buf.append("declare option xdmp:output \"indent=no\";");
        buf.append("declare option xdmp:output \"indent-untyped=no\";");
        buf.append("declare variable $mlmr:splitstart as xs:integer external;");
        buf.append("declare variable $mlmr:splitend as xs:integer external;\n");
        buf.append("let $cols := ");
        buf.append(src);
        buf.append("[$mlmr:splitstart to $mlmr:splitend]");
        buf.append("\nfor $doc in $cols");
        buf.append("\nlet $uri := fn:base-uri($doc)\n return (");
        
        if (copyCollection || copyPermission || copyProperties || copyQuality) {
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
            // if copy-properties, else + (),\n
            if (copyProperties) {
                buf.append("xdmp:document-properties($uri)/prop:properties,\n");
            } else {
                buf.append("(),\n");
            }
            // end-of-record marker
            buf.append("0");
        }
        
        buf.append(" )\n");
        buf.append(",'EOM',"); //end of metadata
        
        //doc
        buf.append(src);
        buf.append("[$mlmr:splitstart to $mlmr:splitend]");
        
        // naked properties
        
        if (copyProperties) {
            buf.append(", if ($mlmr:splitstart eq 1) then ");
            buf.append("\nlet $props := cts:search(");
            if (cFilter != null) {
                buf.append("xdmp:collection-properties(");
                buf.append(cFilter);
                buf.append(")");
            } else if (dFilter != null) {
                buf.append("xdmp:directory-properties(");
                buf.append(dFilter);
                buf.append(", \"infinity\")");
            } else {
                buf.append("xdmp:collection-properties()");
            }
            buf.append(",");
            buf.append("cts:not-query(cts:document-fragment-query(cts:and-query( () ))))\n");
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
            buf.append("$doc/prop:properties, \n");

            // end-of-record marker
            buf.append("0");

            buf.append(" )\n");
            buf.append(" else ()");
        }

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
        while (item != null && ValueType.ELEMENT == item.getItemType()) {
            if (!copyPermission) {
                item = result.next();
                continue;
            }
            try {
                readPermission((XdmElement) item.getItem(), metadata);
            } catch (Exception e) {
                e.printStackTrace();
            }
            item = result.next();
        }
        // handle quality, always present even if not requested (barrier)
        metadata.setQuality((XSInteger) item.getItem());
        item = result.next();

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
            return false;
        }
        ResultItem currItem = null;
        currItem = result.next();

        // naked properties are excluded from count
        if (count < length) {
            count++;
            // docs
            String uri = null;
            uri = currItem.getDocumentURI();
            if (uri == null) {
                throw new IOException("Missing document URI for result "
                    + count);
            }
            currentValue = new MarkLogicDocumentWithMeta();
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
        } else {
            // naked properties
            currentValue = new MarkLogicDocumentWithMeta();
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
        DocumentMetadata _metadata) throws Exception {
        // permission: turn into a ContentPermission object
        // each permission is a sec:permission element.
        // children:
        // sec:capability ("read", "insert", "update")
        // and sec:role xs:unsignedLong (but we need string)
        LOG.debug("permissionElement = " + _permissionElement.asString());

        Element permissionW3cElement = _permissionElement.asW3cElement();
        LOG.debug("permissionElement = " + permissionW3cElement.toString());

        NodeList capabilities = permissionW3cElement
            .getElementsByTagName("sec:capability");
        NodeList roles = permissionW3cElement
            .getElementsByTagName("sec:role-name");
        Node role;
        Node capability;
        if (0 < roles.getLength() && 0 < capabilities.getLength()) {
            role = roles.item(0);
            capability = capabilities.item(0);
            _metadata.addPermission(capability.getTextContent(),
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

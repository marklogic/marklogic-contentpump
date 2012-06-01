/*
 * Copyright 2003-2012 MarkLogic Corporation
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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicDocument;
import com.marklogic.mapreduce.MarkLogicInputSplit;
import com.marklogic.mapreduce.MarkLogicRecordReader;
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
 * can't reuse MarkLogicRecordReader, because the prolog of the query need to be changed, can't simply change query body
 * 
 * @author ali
 *
 */
public class MarkLogicReaderForExportArchive extends MarkLogicRecordReader <DocumentURI, MarkLogicDocument>{
    static final float DOCUMENT_TO_FRAGMENT_RATIO = 1;
    public static final Log LOG = LogFactory
    .getLog(MarkLogicReaderForExportArchive.class);
    protected boolean copyCollection;
    protected boolean copyPermission;
    protected boolean copyProperties;
    protected boolean copyQuality;
    /**
     * Current key.
     */
    protected DocumentURI currentKey;
    /**
     * Current value.
     */
    protected MarkLogicDocument currentValue;

    public MarkLogicReaderForExportArchive(Configuration conf) {
        super(conf);
        copyCollection = isCopyCollection(conf);
        copyPermission = isCopyPermission(conf);
        copyProperties = isCopyProperties(conf);
        copyQuality = isCopyQuality(conf);
        currentKey = new DocumentURI();

    }
    
    private boolean isCopyCollection(Configuration conf){
        String r = conf.get(ConfigConstants.CONF_COPY_COLLECTIONS);
        return Boolean.parseBoolean(r);
    }
    
    private boolean isCopyPermission(Configuration conf){
        String r = conf.get(ConfigConstants.CONF_COPY_PERMISSIONS);
        return Boolean.parseBoolean(r);
    }
    
    private boolean isCopyProperties(Configuration conf){
        String r = conf.get(ConfigConstants.CONF_COPY_PROPERTIES);
        return Boolean.parseBoolean(r);
    }
    
    private boolean isCopyQuality(Configuration conf){
        String r = conf.get(ConfigConstants.CONF_COPY_QUALITY);
        return Boolean.parseBoolean(r);
    }
    
    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        mlSplit = (MarkLogicInputSplit) inSplit;
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
            serverUri = InternalUtilities
                .getInputServerUri(conf, hostNames[0]);
        } catch (URISyntaxException e) {
            LOG.error(e);
            throw new IOException(e);
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

        Collection<String> nsCol = conf.getStringCollection(PATH_NAMESPACE);
        String docExpr = conf.get(ConfigConstants.DOCUMENT_FILTER,
            ConfigConstants.DEFAULT_DOCUMENT_FILTER);
        String query = "for $doc in " + docExpr + "\n";
        query += "let $uri := fn:base-uri($doc)";
        String subExpr = conf.get(SUBDOCUMENT_EXPRESSION, "");
        StringBuilder buf = new StringBuilder();
        buf.append("xquery version \"1.0-ml\"; \n");
        buf.append("declare namespace mlmr=\"http://marklogic.com/hadoop\";");
        buf.append("declare option xdmp:output \"indent=no\";");
        buf.append("declare option xdmp:output \"indent-untyped=no\";");
        buf.append("declare variable $mlmr:splitstart as xs:integer external;");
        buf.append("declare variable $mlmr:splitend as xs:integer external;");
        buf.append("xdmp:with-namespaces((");
        if (nsCol != null) {
            for (Iterator<String> nsIt = nsCol.iterator(); nsIt.hasNext();) {
                String ns = nsIt.next();
                buf.append('"').append(ns).append('"');
                if (nsIt.hasNext()) {
                    buf.append(',');
                }
            }
        }
        buf.append("),fn:unordered(fn:unordered(");

        buf.append("for $doc in ");
        buf.append(docExpr);
        buf.append("[$mlmr:splitstart to $mlmr:splitend]");
        buf.append("\nlet $uri := fn:base-uri($doc)\n return ('DOC',");
        buf.append("$uri,");
        buf.append("$doc,");
        buf.append("0,");

        buf.append("'META',");
        buf.append("$uri,");
        if (copyCollection) {
            buf.append("xdmp:document-get-collections($uri),\n");
        }
        if (copyPermission) {
            buf.append("let $list := xdmp:document-get-permissions($uri)\n");
            buf.append("let $query := \"import module 'http://marklogic.com/xdmp/security' at '/MarkLogic/security.xqy';\n");
            buf.append("declare variable $LIST as element(sec:permissions) external;\n");
            buf.append("for $p in $LIST/sec:permission \n");
            buf.append("return element sec:permission {\n");
            buf.append("$p/@*, $p/node(), sec:get-role-names($p/sec:role-id) }\"\n");
            buf.append("return xdmp:eval($query, (xs:QName('LIST'), element sec:permissions { $list }),");
            buf.append("<options xmlns=\"xdmp:eval\"><database>{ xdmp:security-database() }</database></options>),");
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
        buf.append("0 )\n");

        buf.append(")[");
        buf.append(Long.toString(start));
        buf.append(" to ");
        buf.append(Long.toString(end));
        buf.append("]");
        buf.append(subExpr);
        buf.append("))");
        queryText = buf.toString();

        if (LOG.isDebugEnabled()) {
            LOG.debug(queryText);
        }

        // set up a connection to the server
        try {
            ContentSource cs = InternalUtilities.getInputContentSource(conf,
                serverUri);
            session = cs.newSession("#" + mlSplit.getForestId().toString());
            AdhocQuery aquery = session.newAdhocQuery(queryText);
            aquery.setNewIntegerVariable(MR_NAMESPACE, SPLIT_START_VARNAME,
                start);
            aquery.setNewIntegerVariable(MR_NAMESPACE, SPLIT_END_VARNAME, end);
            RequestOptions options = new RequestOptions();
            options.setCacheResult(false);
            aquery.setOptions(options);
            result = session.submitRequest(aquery);
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
        if (result == null || !result.hasNext()) {
            return false;
        }
            ResultItem item = result.next();
            count++;

            currentValue = new MarkLogicDocument();

            // value type: document or metadata
            String type = null;
            if ( item!=null && item.getItemType() == ValueType.XS_STRING) {
                type = item.asString();
            } else {
                throw new IOException("incorrect format");
            }
            item = result.next();
            String uri = item.asString();
            if(uri == null) {
                throw new IOException("no uri");
            }
            item = result.next();
            if("DOC".equals(type)) {
                currentKey.setUri(uri);
                // handle document-node, always present
//                byte[] content = Utilities.cat(item.asInputStream());
                currentValue.set(item);//content, 0, content.length);
                item = result.next();
            
            } else if ("META".equals(type)) {
                DocumentMetadata metadata = new DocumentMetadata();
                currentKey.setUri(uri + ".metadata");
                // handle collections, may not be present
                while ( item!=null && item.getItemType() == ValueType.XS_STRING) {
                    if (!copyCollection) {
                        continue;
                    }
                    metadata.addCollection(item.asString());
                    item = result.next();
                }
                // handle permissions, may not be present
                while ( item !=null 
                        && ValueType.ELEMENT == item.getItemType()) {
                    if (!copyPermission) {
                        continue;
                    }
                    try {
                        readPermission((XdmElement) item.getItem(),
                                metadata);
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    item = result.next();
                }
                // handle quality, always present even if not requested (barrier)
                metadata.setQuality((XSInteger) item.getItem());
                item = result.next();

                // handle prop:properties node, optional
                // if not present, there will be a 0 as a marker
                if (copyProperties
                        && ValueType.ELEMENT == item.getItemType()) {
                    String pString = item.asString();
                    if (pString != null) {
                        metadata.setProperties(pString);
                    }
                    item = result.next();
                }
                
                byte[] metacontent = metadata.toXML().getBytes();
                currentValue.setXMLFromBytes(metacontent);
                
            } else {
                throw new IOException ("incorrect type");
            }
            
        if (ValueType.XS_INTEGER != item.getItemType()) {
            throw new IOException("unexpected " + item.getItemType() + " "
                + item.asString() + ", expected " + ValueType.XS_INTEGER
                + " 0");
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
    LOG.debug("permissionElement = "
            + _permissionElement.asString());

        Element permissionW3cElement = _permissionElement
                .asW3cElement();
        LOG.debug("permissionElement = "
                + permissionW3cElement.toString());

        NodeList capabilities = permissionW3cElement
                .getElementsByTagName("sec:capability");
        NodeList roles = permissionW3cElement
                .getElementsByTagName("sec:role-name");
        Node role;
        Node capability;
        if (0 < roles.getLength() && 0 < capabilities.getLength()) {
            role = roles.item(0);
            capability = capabilities.item(0);
            _metadata.addPermission(capability.getTextContent(), role
                    .getTextContent());
            if (roles.getLength() > 1) {
                LOG.warn("input permission: "
                        + permissionW3cElement + ": "
                        + roles.getLength() + " roles, using only 1");
            }
            if (capabilities.getLength() > 1) {
                LOG.warn("input permission: "
                        + permissionW3cElement + ": "
                        + capabilities.getLength()
                        + " capabilities, using only 1");
            }
        } else {
            // warn and skip
            if (roles.getLength() < 1) {
                LOG.warn("skipping input permission: "
                        + permissionW3cElement + ": no roles");
            }
            if (capabilities.getLength() < 1) {
                LOG.warn("skipping input permission: "
                        + permissionW3cElement + ": no capabilities");
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
    public DocumentURI getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public MarkLogicDocument getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    
}

/*
 * Copyright 2003-2014 MarkLogic Corporation
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
import java.util.Set;
import java.util.Stack;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.contentpump.utilities.IdGenerator;

/**
 * Reader for AggregateXMLInputFormat.
 * 
 * @author ali
 *
 * @param <VALUEIN>
 */
public class AggregateXMLReader<VALUEIN> extends ImportRecordReader<VALUEIN> {
    public static final Log LOG = LogFactory.getLog(AggregateXMLReader.class);
    public static String DEFAULT_NS = null;
    private int currDepth = 0;
    protected XMLStreamReader xmlSR;

    protected String recordName;
    protected String recordNamespace;
    private int recordDepth = Integer.MAX_VALUE;
    private StringBuilder buffer;
    protected String idName;
    protected String currentId = null;
    private boolean keepGoing = true;
    protected HashMap<String, Stack<String>> nameSpaces = 
        new HashMap<String, Stack<String>>();
    protected boolean startOfRecord = true;
    protected boolean hasNext = true;
    private boolean newDoc = false;
    private boolean newUriId = false;
    protected boolean useAutomaticId = false;
    protected String mode;
    protected IdGenerator idGen;

    protected XMLInputFactory f;
    protected FSDataInputStream fInputStream;
    protected long start;
    protected long pos;
    protected boolean overflow;
    protected long end;
    protected boolean compressed = false;
    
    public AggregateXMLReader() {
    }

    @Override
    public void close() throws IOException {
        if (xmlSR != null) {
            try {
                xmlSR.close();
            } catch (XMLStreamException e) {
                LOG.error("Error closing stream reader", e);
            }
        }
        if (fInputStream != null) {
            fInputStream.close();
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (!hasNext) {
            return 1;
        }
        return (pos > end) ? 1 : ((float) (pos - start)) / (end - start);
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        initConfig(context);
        initAggConf(context);
        
        f = XMLInputFactory.newInstance();
        file = ((FileSplit) inSplit).getPath();
        fs = file.getFileSystem(context.getConfiguration());
        FileStatus status = fs.getFileStatus(file);
        if(status.isDir()) {
            iterator = new FileIterator((FileSplit)inSplit, context);
            inSplit = iterator.next();
        }
        initStreamReader(inSplit);
    }
    
    protected void initStreamReader(InputSplit inSplit) throws IOException,
        InterruptedException {
        start = 0;
        end = inSplit.getLength();
        overflow = false;
        file = ((FileSplit) inSplit).getPath();
        configFileNameAsCollection(conf, file);

        fInputStream = fs.open(file);

        try {
            xmlSR = f.createXMLStreamReader(fInputStream, encoding);
        } catch (XMLStreamException e) {
            LOG.error(e.getMessage(), e);
        }

        if (useAutomaticId) {
            idGen = new IdGenerator(file.toUri().getPath() + "-"
                + ((FileSplit) inSplit).getStart());
        }
    }
    
    protected void initAggConf(TaskAttemptContext context) {
        Configuration conf = context.getConfiguration();
        idName = conf.get(ConfigConstants.CONF_AGGREGATE_URI_ID);
        if (idName == null) {
            useAutomaticId = true;
        }
        recordName = conf.get(ConfigConstants.CONF_AGGREGATE_RECORD_ELEMENT);
        recordNamespace = conf
            .get(ConfigConstants.CONF_AGGREGATE_RECORD_NAMESPACE);

    }

    private void write(String str) {
        if (buffer == null) {
            buffer = new StringBuilder();
        }
        if (newDoc && currDepth >= recordDepth) {
            buffer.append(str);
        }

    }
    
    protected void copyNameSpaceDecl() {
        if (recordDepth < currDepth) {
            return;
        }
        int stop = xmlSR.getNamespaceCount();
        if (stop > 0) {
            String nsDeclPrefix, nsDeclUri;
            if (LOG.isTraceEnabled()) {
                LOG.trace("checking namespace declarations");
            }
            for (int i = 0; i < stop; i++) {
                nsDeclPrefix = xmlSR.getNamespacePrefix(i);
                nsDeclUri = xmlSR.getNamespaceURI(i);
                if (LOG.isTraceEnabled()) {
                    LOG.trace(nsDeclPrefix + ":" + nsDeclUri);
                }
                if (nameSpaces.containsKey(nsDeclPrefix)) {
                    nameSpaces.get(nsDeclPrefix).push(nsDeclUri);
                } else {
                    Stack<String> s = new Stack<String>();
                    s.push(nsDeclUri);
                    nameSpaces.put(nsDeclPrefix, s);
                }
            }
        }
    }
    
    protected void removeNameSpaceDecl() {
        if (recordDepth < currDepth) {
            return;
        }
        int stop = xmlSR.getNamespaceCount();
        if (stop > 0) {
            String nsDeclPrefix;
            if (LOG.isTraceEnabled()) {
                LOG.trace("checking namespace declarations");
            }           
            for (int i = 0; i < stop; i++) {
                nsDeclPrefix = xmlSR.getNamespacePrefix(i);
                if (nameSpaces.containsKey(nsDeclPrefix)) {
                    if (!nameSpaces.get(nsDeclPrefix).isEmpty()) {
                        nameSpaces.get(nsDeclPrefix).pop();
                    }
                } else {
                    LOG.warn("Namespace " + nsDeclPrefix + " not in scope");
                }
            }
        }
    }

    private void processStartElement() throws XMLStreamException {
        String name = xmlSR.getLocalName();
        String namespace = xmlSR.getNamespaceURI();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Start-tag: " + xmlSR.getName() + " at depth " + currDepth);
        }
        if (namespace == null) {
            String prefix = xmlSR.getPrefix();
            if ("".equals(prefix)) {
                prefix = DEFAULT_NS;
            } 
            if (nameSpaces.get(prefix) != null) {
                namespace = nameSpaces.get(prefix).peek();
            }
        }
        
        String prefix = xmlSR.getPrefix();
        int attrCount = xmlSR.getAttributeCount();
        boolean isNewRootStart = false;
        currDepth++;
        if (recordName == null) {
            recordName = name;
            if (recordNamespace == null) {
                recordNamespace = namespace;
            }
            recordDepth = currDepth;
            isNewRootStart = true;
            newDoc = true;
            newUriId = true;
            if (useAutomaticId) {
                setKey(idGen.incrementAndGet());
            }
        } else {
            // record element name may not nest
            if (name.equals(recordName)
                && ((recordNamespace == null && namespace == null) 
                     || (recordNamespace != null && recordNamespace
                                        .equals(namespace)))) {
                recordDepth = currDepth;
                isNewRootStart = true;
                newDoc = true;
                newUriId = true;
                if (useAutomaticId) {
                    setKey(idGen.incrementAndGet());
                }
            }
        }
        
        copyNameSpaceDecl();

        if (!newDoc) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("<");
        if (prefix != null && !prefix.equals("")) {
            sb.append(prefix + ":" + name);
        } else {
            sb.append(name);
        }
        // add namespaces declared into the new root element
        if (isNewRootStart && namespace != null) {
            Set<String> keys = nameSpaces.keySet();
            for (String k : keys) {
                String v = nameSpaces.get(k).peek();
                if (DEFAULT_NS == k) {
                    sb.append(" xmlns=\"" + v + "\"");
                } else {
                    sb.append(" xmlns:" + k + "=\"" + v + "\"");
                }
            }
        }

        for (int i = 0; i < attrCount; i++) {
            String aPrefix = xmlSR.getAttributePrefix(i);
            String aName = xmlSR.getAttributeLocalName(i);
            String aValue = StringEscapeUtils.escapeXml(xmlSR
                .getAttributeValue(i));
            sb.append(" " + (null == aPrefix ? "" : (aPrefix + ":")) + aName
                + "=\"" + aValue + "\"");
            if (!useAutomaticId && newDoc && ("@" + aName).equals(idName)) {
                currentId = aValue;
                setKey(aValue);
            }
        }
        sb.append(">");

        // allow for repeated idName elements: first one wins
        // NOTE: idName is namespace-insensitive
        if (!useAutomaticId && newDoc && name.equals(idName)) {
            if (xmlSR.next() != XMLStreamConstants.CHARACTERS) {
                throw new XMLStreamException("badly formed xml or " + idName
                    + " is not a simple node: at" + xmlSR.getLocation());
            }

            String newId = xmlSR.getText();
            currentId = newId;
            sb.append(newId);
            if (newUriId) {
                setKey(newId);
                newUriId = false;
            } else {
                LOG.warn("Ignoring duplicate URI_ID: " + idName + " = " + 
                        newId);
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("URI_ID: " + newId);
            }
            // advance to the END_ELEMENT
            if (xmlSR.next() != XMLStreamConstants.END_ELEMENT) {
                throw new XMLStreamException(
                    "badly formed xml: no END_TAG after id text"
                        + xmlSR.getLocation());
            }
            sb.append("</");
            sb.append(idName);
            sb.append(">");
            currDepth--;
        }
        
        write(sb.toString());
        
    }

    /**
     * 
     * @return false when the record end-element is found; true when keep going
     * @throws XMLStreamException
     */
    @SuppressWarnings("unchecked")
    private boolean processEndElement() throws XMLStreamException {
        String name = xmlSR.getLocalName();
        String namespace = xmlSR.getNamespaceURI();
        if (LOG.isTraceEnabled()) {
            LOG.trace("End-tag: " + xmlSR.getName() + " at depth " + currDepth);
        }
        if (namespace == null) {
            String prefix = xmlSR.getPrefix();
            if ("".equals(prefix)) {
                prefix = DEFAULT_NS;
            } 
            if (nameSpaces.get(prefix) != null && 
                !nameSpaces.get(prefix).isEmpty()) {
                namespace = nameSpaces.get(prefix).peek();
            }
        }
        String prefix = xmlSR.getPrefix();
        StringBuilder sb = new StringBuilder();
        sb.append("</");
        if (prefix != null && prefix != "") {
            sb.append(prefix + ":" + name);
        } else {
            sb.append(name);
        }
        sb.append(">");
        //write to finish the end tag before checking errors
        write(sb.toString()); 
        if (!newDoc || !name.equals(recordName) || !((recordNamespace == null && namespace == null) 
          || (recordNamespace != null && recordNamespace
          .equals(namespace)))) {
         // not the end of the record: go look for more nodes
            
            if( currDepth == 1) {
                cleanupEndElement();
            } else {
                removeNameSpaceDecl();
                currDepth--;
            }
            return true;
        }
        if (!useAutomaticId && null == currentId && newDoc) {
            LOG.error("end of record element " + name
                + " with no id found: " + ConfigConstants.AGGREGATE_URI_ID
                + "=" + idName);
            cleanupEndElement();
            return true;
        }

        if (value instanceof Text) {
            ((Text) value).set(buffer.toString());
        } else if (value instanceof ContentWithFileNameWritable) {
            VALUEIN realValue = ((ContentWithFileNameWritable<VALUEIN>) value)
                .getValue();
            if (realValue instanceof Text) {
                ((Text) realValue).set(buffer.toString());
            } else {
                LOG.error("Expects Text in aggregate XML, but gets "
                    + realValue.getClass().getCanonicalName());
                key = null;
            }
        } else {
            LOG.error("Expects Text in aggregate XML, but gets "
                + value.getClass().getCanonicalName());
            key = null;
        }
        
        cleanupEndElement();
        // end of new record
        return false;
    }
    
    protected void cleanupEndElement(){
        currentId = null;
        newDoc = false;
        // reset buffer
        buffer.setLength(0);
        removeNameSpaceDecl();
        currDepth--;        
    }
 
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (xmlSR == null) {
            hasNext = false;
            return false;
        }

        try {
            while (xmlSR.hasNext()) {
                int eventType;
                //getCharacterOffset() returns int; 
                //int will overflows if file is larger than 2GB
                if (!overflow && xmlSR.getLocation().getCharacterOffset() < -1) {
                    overflow = true;
                    LOG.info("In progress...");
                }
                //do not update pos if offset overflows
                if (!overflow) {
                    pos = xmlSR.getLocation().getCharacterOffset();
                }
                eventType = xmlSR.next();
                switch (eventType) {
                case XMLStreamConstants.START_ELEMENT:
                    if (startOfRecord) {
                        // this is the start of the root, only copy
                        // namespaces
                        copyNameSpaceDecl();
                        startOfRecord = false;
                        continue;
                    }
                    processStartElement();
                    break;
                case XMLStreamConstants.CHARACTERS:
                    write(StringEscapeUtils.escapeXml(xmlSR.getText()));
                    break;
                case XMLStreamConstants.CDATA:
                    write("<![CDATA[");
                    write(xmlSR.getText());
                    write("]]>");
                    break;
                case XMLStreamConstants.SPACE:
                    write(xmlSR.getText());
                    break;
                case XMLStreamConstants.ENTITY_REFERENCE:
                    write("&");
                    write(xmlSR.getLocalName());
                    write(";");
                    break;
                case XMLStreamConstants.DTD:
                    write("<!DOCTYPE");
                    write(xmlSR.getText());
                    write(">");
                    break;
                case XMLStreamConstants.PROCESSING_INSTRUCTION:
                    write("<?");
                    write(xmlSR.getPIData());
                    write("?>");
                    break;
                case XMLStreamConstants.COMMENT:
                    write("<!--");
                    write(xmlSR.getText());
                    write("-->");
                    break;
                case XMLStreamConstants.END_ELEMENT:
                    keepGoing = processEndElement();
                    if (!keepGoing) {
                        keepGoing = true;
                        return true;
                    }
                    break;
                case XMLStreamConstants.START_DOCUMENT:
                    throw new XMLStreamException(
                        "unexpected start of document within record!\n"
                            + "recordName = " + recordName
                            + ", recordNamespace = " + recordNamespace
                            + " at " + xmlSR.getLocation());
                case XMLStreamConstants.END_DOCUMENT:
                    if (currentId != null) {
                        throw new XMLStreamException(
                            "end of document before end of current record!\n"
                                + "recordName = " + recordName
                                + ", recordNamespace = " + recordNamespace
                                + " at " + xmlSR.getLocation());
                    } else {
                        if(compressed) {
                            //this doc is done, refer to the zip for next doc
                            hasNext = false;
                            return false;
                        } else {
                            //get next file from FileIterator
                            if (iterator!=null && iterator.hasNext()) {
                                close();
                                initStreamReader(iterator.next());
                                continue;
                            } else {
                                hasNext = false;
                                return false;
                            }
                        }
                    }
                default:
                    throw new XMLStreamException("UNIMPLEMENTED: " + eventType);
                }
            }
        } catch (XMLStreamException e) {
            LOG.error("Parsing error", e);
            throw new IOException("Parsing error", e);
        }

        return false;
    }
    
    @Override
    protected void setKey(String val) {
        if (val == null) {
            key = null;
        } else {
            String uri = getEncodedURI(val);
            super.setKey(uri);
        }
    }
}

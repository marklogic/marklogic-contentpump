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
import java.util.HashMap;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class AggregateXMLReader<VALUEIN> extends ImportRecordReader<VALUEIN> {
    protected static Pattern[] patterns = new Pattern[] {
        Pattern.compile("&"), Pattern.compile("<"), Pattern.compile(">") };
    private static String DEFAULT_NS = null;
    private int currDepth = 0;
    protected XMLStreamReader xmlSR;
    protected String recordName;
    protected String recordNamespace;
    private int recordDepth = Integer.MAX_VALUE;
    private StringBuilder buffer;
    protected String idName;
    private boolean skippingRecord = false;
    protected String currentId = null;
    private boolean keepGoing = true;
    protected HashMap<String, Stack<String>> nameSpaces = new HashMap<String, Stack<String>>();
    protected boolean startOfRecord = true;
    protected boolean hasNext = true;
    private boolean newDoc = false;
    protected boolean useAutomaticId = false;
    protected String mode;
    protected IdGenerator idGen;
    public AggregateXMLReader() {

    }

    @Override
    public void close() throws IOException {
        if (xmlSR != null) {
            try {
                xmlSR.close();
            } catch (XMLStreamException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext ? 0 : 1;
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path file = ((FileSplit) inSplit).getPath();
        initCommonConfigurations(conf, file);
        configFileNameAsCollection(conf, file);
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);
        XMLInputFactory f = XMLInputFactory.newInstance();
        try {
            xmlSR = f.createXMLStreamReader(fileIn);
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        initAggConf(inSplit, context);
    }
    
    protected void initAggConf(InputSplit inSplit, TaskAttemptContext context) {
        Configuration conf = context.getConfiguration();
        idName = conf.get(ConfigConstants.CONF_AGGREGATE_URI_ID);
        if (idName == null) {
            useAutomaticId = true;
        }
        recordName = conf.get(ConfigConstants.CONF_AGGREGATE_RECORD_ELEMENT);
        recordNamespace = conf
            .get(ConfigConstants.CONF_AGGREGATE_RECORD_NAMESPACE);
        if (useAutomaticId) {
            mode = conf.get(ConfigConstants.CONF_MODE);
            if (mode.equals(ConfigConstants.MODE_DISTRIBUTED)) {
                idGen = new LocalIdGenerator(String.valueOf(context
                    .getTaskAttemptID().getTaskID().getId()));
            } else if (mode.equals(ConfigConstants.MODE_LOCAL)) {
                idGen = new LocalIdGenerator(
                    String.valueOf(inSplit.hashCode()));
            }
        }
    }

    private void write(String str) {
        if (skippingRecord) {
            return;
        }
        if (buffer == null) {
            buffer = new StringBuilder();
        }
        if (recordDepth <= currDepth) {
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
            LOG.debug("checking namespace declarations");
            for (int i = 0; i < stop; i++) {
                nsDeclPrefix = xmlSR.getNamespacePrefix(i);
                nsDeclUri = xmlSR.getNamespaceURI(i);
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
            LOG.debug("checking namespace declarations");
            for (int i = 0; i < stop; i++) {
                nsDeclPrefix = xmlSR.getNamespacePrefix(i);
                if (nameSpaces.containsKey(nsDeclPrefix)) {
                    if (!nameSpaces.get(nsDeclPrefix).isEmpty()) {
                        nameSpaces.get(nsDeclPrefix).pop();
                    }
                } else {
                    LOG.warn("Namespace " + nsDeclPrefix + " in scope");
                }
            }
        }
    }

    private void processStartElement() throws XMLStreamException {
        String name = xmlSR.getLocalName();
        String namespace = xmlSR.getNamespaceURI();
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
                if (useAutomaticId) {
                    setKey(idGen.incrementAndGet());
                }
            }
        }
        
        copyNameSpaceDecl();

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
            String aValue = escapeXml(xmlSR.getAttributeValue(i));
            sb.append(" " + (null == aPrefix ? "" : aPrefix + ":") + aName
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
            setKey(newId);

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

        // TODO
        // if the startId is still defined, and the uri has been found,
        // we should skip as much of this work as possible
        // this avoids OutOfMemory errors, too
        // if (skippingRecord) {
        // LOG.debug("skipping record");
        // return;
        // }
        write(sb.toString());
    }

    /**
     * 
     * @return false when the record end-element is found; true when keep going
     * @throws XMLStreamException
     */
    @SuppressWarnings("unchecked")
    private boolean processEndElement() throws XMLStreamException {
        if (skippingRecord) {
            return false;
        }
        
        String name = xmlSR.getLocalName();
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

        if (recordDepth != currDepth ) {
            // not the end of the record: go look for more nodes
            currDepth--;
            removeNameSpaceDecl();
            return true;
        } else if (!newDoc) {
            //depth is same: it is record that not matched
            cleanupEndElement();
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
        currDepth--;
        removeNameSpaceDecl();
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
                eventType = xmlSR.next();

                switch (eventType) {
                case XMLStreamConstants.START_ELEMENT:
                    if (startOfRecord) {
                        // this is the start of the root, only copy namespaces
                        copyNameSpaceDecl();
                        startOfRecord = false;
                        continue;
                    }
                    processStartElement();
                    break;
                case XMLStreamConstants.CHARACTERS:
                    write(escapeXml(xmlSR.getText()));
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
                        hasNext = false;
                        break;
                    }
                default:
                    throw new XMLStreamException("UNIMPLEMENTED: " + eventType);
                }
            }
        } catch (XMLStreamException e) {
            LOG.warn(e.getClass().getSimpleName() + " at "
                + xmlSR.getLocation());// .getPositionDescription());
            if (e.getMessage().contains("quotation or apostrophe")
            /* && !config.isFatalErrors() */) {
                // messed-up attribute? skip it?
                LOG.warn("attribute error: " + e.getMessage());
                // all we can do is ignore it, apparently
            } else {
                LOG.error(e.toString());
                throw new IOException(e.toString());
            }
        }
        return false;
    }
    
    public static String escapeXml(String _in) {
        if (null == _in){
            return "";
        }
        return patterns[2].matcher(
                patterns[1].matcher(
                        patterns[0].matcher(_in).replaceAll("&amp;"))
                        .replaceAll("&lt;")).replaceAll("&gt;");
    }
}

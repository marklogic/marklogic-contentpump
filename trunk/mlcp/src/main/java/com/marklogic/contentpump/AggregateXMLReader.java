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
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
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

import com.marklogic.contentpump.utilities.AggregateXMLInputFactoryImpl;
import com.marklogic.contentpump.utilities.IdGenerator;
import com.marklogic.contentpump.utilities.LocalIdGenerator;
import com.sun.org.apache.xerces.internal.impl.XMLDocumentScannerImpl;
import com.sun.org.apache.xerces.internal.impl.XMLStreamReaderImpl;
import com.sun.org.apache.xerces.internal.xni.NamespaceContext;

public class AggregateXMLReader<VALUEIN> extends ImportRecordReader<VALUEIN> {

    protected static Pattern[] patterns = new Pattern[] {
        Pattern.compile("&"), Pattern.compile("<"), Pattern.compile(">") };
    public static String DEFAULT_NS = null;
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
    protected NamespaceContext nsctx;
    protected boolean startOfRecord = true;
    protected boolean hasNext = true;
    private boolean newDoc = false;
    private boolean newUriId = false;
    protected boolean useAutomaticId = false;
    protected String mode;
    protected IdGenerator idGen;
    protected boolean parallelMode = false;
    protected Configuration conf;
    protected XMLInputFactory f;
    protected FileSystem fs;
    protected Path file;
    protected FSDataInputStream fInputStream;
    protected long start;
    protected long pos;
    protected long end;
    protected long parserOffsetInSplit;

    protected int LOOKAHEAD_BUF = 4096;

    protected static String NOT_WELL_FORMED = "must be well-formed";
    protected static String CONTENT_IN_PROLOG = "Content is not allowed in "
        + "prolog";
    
    public AggregateXMLReader(String recordElem, String namespace,
        NamespaceContext nsctx) {
        recordName = recordElem;
        recordNamespace = namespace;
        this.nsctx = nsctx;
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
        return (pos > end) ? 1 : ((float) (pos - start)) / (end - start);
    }

    @Override
    public void initialize(InputSplit is, TaskAttemptContext context)
        throws IOException, InterruptedException {
        conf = context.getConfiguration();
        parallelMode = conf.getBoolean(ConfigConstants.CONF_AGGREGATE_SPLIT, false);
        if (parallelMode) {
            AggregateSplit asplit = (AggregateSplit) is;
            initAggConf(asplit, context);
            nameSpaces = asplit.getNamespaces();
            recordName = asplit.getRecordElem();
            nsctx = asplit.getNamespaceContext();
            file = asplit.getPath();
            start = asplit.getStart();
            end = start + asplit.getLength();
            initCommonConfigurations(conf, file);
            configFileNameAsCollection(conf, file);
            fs = file.getFileSystem(context.getConfiguration());
            fInputStream = fs.open(file);
            f = new AggregateXMLInputFactoryImpl();
            long fileLen = fs.getFileStatus(file).getLen();
            if (asplit.getStart() == 0 && fileLen == asplit.getLength()) {
                try {
                    xmlSR = f.createXMLStreamReader(fInputStream);
                } catch (XMLStreamException e) {
                    e.printStackTrace();
                }
            } else {
                // jump to the first record elem in the split
                moveToNextRecord(start);
            }
        } else {
            start = 0;
            end = is.getLength();
            initializeSeqMode(is, context);
        }
    }
    
    public void initializeSeqMode(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        file = ((FileSplit) inSplit).getPath();
        initCommonConfigurations(conf, file);
        configFileNameAsCollection(conf, file);
        fs = file.getFileSystem(context.getConfiguration());
        fInputStream = fs.open(file);
        
        f = new AggregateXMLInputFactoryImpl();
        try {
            xmlSR = f.createXMLStreamReader(fInputStream);
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
        if (parallelMode) {
            //parallel mode always starts from record elem
            buffer.append(str);
        } else if (currDepth >= recordDepth) {
            buffer.append(str);
        }

    }
    
    protected void copyNameSpaceDecl() {
        if (!parallelMode && recordDepth < currDepth) {
            return;
        }
        int stop = xmlSR.getNamespaceCount();
        if (stop > 0) {
            String nsDeclPrefix, nsDeclUri;
            if (LOG.isDebugEnabled()) {
                LOG.debug("checking namespace declarations");
            }
            for (int i = 0; i < stop; i++) {
                nsDeclPrefix = xmlSR.getNamespacePrefix(i);
                nsDeclUri = xmlSR.getNamespaceURI(i);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(nsDeclPrefix + ":" + nsDeclUri);
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
        if (!parallelMode && recordDepth < currDepth) {
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start-tag: " + name);
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
            if (newUriId) {
                setKey(newId);
                newUriId = false;
            } else {
                LOG.warn("duplicate URI_ID: " + idName + ". Skipped. " );
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("URI_ID: " + newId);
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
        String namespace = xmlSR.getNamespaceURI();
        if (LOG.isDebugEnabled()) {
            LOG.debug("End-tag: " + name);
        }
        if (namespace == null) {
            String prefix = xmlSR.getPrefix();
            if ("".equals(prefix)) {
                prefix = DEFAULT_NS;
            } 
            if (nameSpaces.get(prefix) != null && !nameSpaces.get(prefix).isEmpty()) {
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
                currDepth--;
                removeNameSpaceDecl();
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
        currDepth--;
        removeNameSpaceDecl();
    }
 
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (xmlSR == null) {
            hasNext = false;
            return false;
        }
        
        while (!parallelMode || (parallelMode && pos <= end)) {
            try {
                while (xmlSR.hasNext()) {
                    int eventType;
                    pos = xmlSR.getLocation().getCharacterOffset() + parserOffsetInSplit;
                    eventType = xmlSR.next();
                    switch (eventType) {
                    case XMLStreamConstants.START_ELEMENT:
                        if (!parallelMode && startOfRecord) {
                            // this is the start of the root, only copy
                            // namespaces
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
                            return false;
                        }
                    default:
                        throw new XMLStreamException("UNIMPLEMENTED: "
                            + eventType);
                    }
                }
            } catch (XMLStreamException e) {
                if (e.getMessage().contains("quotation or apostrophe")) {
                    // messed-up attribute? skip it?
                    LOG.warn("attribute error: " + e.getMessage());
                    continue;
                    // all we can do is ignore it, apparently
                } else if (e.getMessage().contains(CONTENT_IN_PROLOG)
                    || e.getMessage().contains(NOT_WELL_FORMED)) {

                    if (parallelMode) {
                        long nextLoc = xmlSR.getLocation()
                            .getCharacterOffset() + parserOffsetInSplit - 32;
                        if (nextLoc > fInputStream.getPos()) {
                            // abnormal/rare parser location (maybe a bug in
                            // jdk, happens once in 9 million medline)
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Parser position " + nextLoc
                                    + " > input stream position "
                                    + fInputStream.getPos());
                            }
                            //don't know where to end, move conservatively
                            nextLoc = parserOffsetInSplit
                                + recordName.length() + 2;
                        }

                        if (LOG.isDebugEnabled()) {
                            LOG.debug(e.getMessage());
                            LOG.debug("Next postion to search " + nextLoc);
                        }
                        if (moveToNextRecord(nextLoc) == -1) {
                            return false;
                        }
                        continue;
                    }
                }

                LOG.error(e.toString() + "\n" + e.getStackTrace());
                throw new IOException(e.toString());
            }
        }
        return false;
    }
    
    /**
     * 
     * @param match
     * @return the offset of the match found; -1 if not found
     * @throws IOException
     */
    protected int readUntilMatch(String match, long streamOffset) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Regex pattern for record element: " + new String(match));
        }
        fInputStream.seek(streamOffset);
        Pattern pattern = Pattern.compile(match);
        while (true) {
            byte buffer[] = new byte[LOOKAHEAD_BUF];
            int b = fInputStream.read(buffer);
            // end of file:
            if (b == -1) {
                return -1;
            }
            Matcher m = pattern.matcher(new String(buffer));
            if (m.find()) {
                return m.start();
            } else {
                //be conservative: move forward recordName length's bytes
                streamOffset += recordName.length();
                if (streamOffset < end) {
                    fInputStream.seek(streamOffset);
                }
            }
        }
    }
    /**
     * Move to the beginning of record element
     * @return the offset of the next record element;
     * @throws IOException
     */
    protected long moveToNextRecord(long streamOffset) throws IOException {
        String matchStr = "<(?!(/))(.+:)?" + recordName + "[ >]";
        try {
            //close xml stream 
            if (xmlSR!=null ) {
                xmlSR.close();
            }
            if (fInputStream!=null) fInputStream.close();
            fInputStream = fs.open(file);
            int offset = readUntilMatch(matchStr, streamOffset);
            if (offset == -1) {
                //no record elem in this split
                return -1;
            }
        
            pos = offset + streamOffset;
            parserOffsetInSplit = pos;
            fInputStream.seek(pos);
            xmlSR = f.createXMLStreamReader(fInputStream);
            updateNamespaceContext();

            return pos;
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        return -1;
    }

    protected void updateNamespaceContext() {
        //to hide the println made by getScanner in jdk
        PrintStream printStreamOriginal = System.out;
        System.setOut(new PrintStream(new OutputStream() {
            public void write(int b) {
            }
        }));
        XMLDocumentScannerImpl scanner = ((XMLStreamReaderImpl) xmlSR)
            .getScanner();
        System.setOut(printStreamOriginal);
        //set namespace context in xmlSR
        scanner.setProperty(
            "http://apache.org/xml/properties/internal/namespace-context",
            nsctx);
        int count = scanner.getNamespaceContext().getDeclaredPrefixCount();
        if (LOG.isDebugEnabled()) {
            LOG.debug("namespaces in context");
            for (int i = 0; i < count; i++) {
                String p = scanner.getNamespaceContext()
                    .getDeclaredPrefixAt(i);
                LOG.debug("prefix:" + p);
                LOG.debug(scanner.getNamespaceContext().getURI(p));
            }
        }
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
    
    @Override
    protected void setKey(String val) {
        String uri = getEncodedURI(val);
        if (uri == null) {
            key = null;
        } else {
            super.setKey(uri);
        }
    }
}

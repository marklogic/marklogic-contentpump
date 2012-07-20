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
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.mapreduce.DocumentURI;
import com.sun.org.apache.xerces.internal.util.NamespaceContextWrapper;
import com.sun.org.apache.xerces.internal.util.NamespaceSupport;
import com.sun.org.apache.xerces.internal.xni.NamespaceContext;

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class AggregateXMLAdvInputFormat extends FileAndDirectoryInputFormat<DocumentURI, Text> {
    public static final Log LOG = LogFactory
    .getLog(AggregateXMLAdvInputFormat.class);
//    public static final String START_TAG_KEY = "xmlinput.start";
//    public static final String END_TAG_KEY = "xmlinput.end";
    private int currDepth = 0;
    protected XMLStreamReader xmlSR;
    protected String recordName;
    protected String recordNamespace;
    private int recordDepth = Integer.MAX_VALUE;
    private StringBuilder buffer;
    private boolean skippingRecord = false;
    protected String currentId = null;
    private boolean keepGoing = true;
    protected boolean startOfRecord = true;
    protected boolean hasNext = true;
    protected HashMap<String, Stack<String>> nameSpaces;
    protected NamespaceContext nsctx;
    
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }
    
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
//        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
//        long maxSize = getMaxSplitSize(job);     
        
        List<InputSplit> splits = super.getSplits(job);
        // add namespace info into splits
        List<InputSplit> populatedSplits = new ArrayList<InputSplit>();
        
        LOG.info(splits.size() + " splits generated");
        
        for (InputSplit file: splits) {
            Path path = ((FileSplit)file).getPath();
            if (LOG.isDebugEnabled()) {
                LOG.debug(path.getName());
            }
            long position = ((FileSplit)file).getStart();
            FileSystem fs = path.getFileSystem(job.getConfiguration());
            FSDataInputStream fileIn = fs.open(path);
            FileStatus status = fs.getFileStatus(path);
            if (position == 0) {
                // the first split of the file
                nameSpaces = new HashMap<String, Stack<String>>();
                XMLInputFactory f = new AggregateXMLInputFactoryImpl();
                try {
                    xmlSR = f.createXMLStreamReader(fileIn);
                    initAggConf(file, job.getConfiguration());
                    parse();
                    startOfRecord = true;
                    xmlSR.close();
                    fileIn.close();
                } catch (XMLStreamException e) {
                    e.printStackTrace();
                }
            }

             long length = status.getLen();
            if (length != 0) {
                //make a new copy, so that splits in the same agg file won't share the same context
                NamespaceSupport nssp = (NamespaceSupport) nsctx;
                nsctx = new NamespaceSupportAggregate();
                if (nssp != null) {
                    int nscount = nssp.getDeclaredPrefixCount();
                    for(int i=0; i<nscount; i++) {
                        String prefix = nssp.getDeclaredPrefixAt(i);
                        nsctx.declarePrefix(prefix, nssp.getURI(prefix));
                    }
                }
//                System.out.println("GetSplit#namespace context: " + nsctx +":" +  nsctx.getDeclaredPrefixCount());
                AggregateSplit split = new AggregateSplit(((FileSplit)file), nameSpaces, recordName, nsctx);
//                System.out.println("GetSplit#namespace context: " + nsctx +":" + nsctx.getURI("ml"));
//                System.out.println("GetSplit#namespace context: " + nsctx +":" + nsctx.getURI(""));
                populatedSplits.add(split);

            }
        }
        
        return populatedSplits;
        
    }


protected void initAggConf(InputSplit inSplit, Configuration conf) {
    recordName = conf.get(ConfigConstants.CONF_AGGREGATE_RECORD_ELEMENT);
    recordNamespace = conf
        .get(ConfigConstants.CONF_AGGREGATE_RECORD_NAMESPACE);
}

    protected boolean parse() throws IOException {
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
                    if (processStartElement() == true) {
                        return false;
                    }
                    break;
                case XMLStreamConstants.CHARACTERS:
                    write(AggregateXMLReader.escapeXml(xmlSR.getText()));
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
    
    /**
     * 
     * @return true once recordName is set 
     * @throws XMLStreamException
     */
    private boolean processStartElement() throws XMLStreamException {
        String name = xmlSR.getLocalName();
        String namespace = xmlSR.getNamespaceURI();
        currDepth++;
        copyNameSpaceDecl();
        if (recordName == null || (name.equals(recordName)
            && ((recordNamespace == null && namespace == null) 
                || (recordNamespace != null && recordNamespace
                                   .equals(namespace)))) ) {
            recordName = name;
            if (recordNamespace == null) {
                recordNamespace = namespace;
            }
            recordDepth = currDepth;
            //NamespaceContextWrapper belongs to javax.xml.namespace
            //inside the wrapper, it is NamespaceSupport which belongs xerces
            NamespaceSupport nssp = (NamespaceSupport) ((NamespaceContextWrapper) xmlSR.getNamespaceContext()).getNamespaceContext();
            //make a copy of context to avoid data race
            nsctx = new NamespaceSupportAggregate();
            Enumeration<String> itr = nssp.getAllPrefixes();
            while(itr.hasMoreElements()) {
                String prefix = itr.nextElement();
                nsctx.declarePrefix(prefix, nssp.getURI(prefix));
            }
//            newDoc = true;
            return true;
        } 
        return false;
    }
    
    private boolean processEndElement() throws XMLStreamException {
        if (skippingRecord) {
            return false;
        }
        removeNameSpaceDecl();
        return true;
    }
    
    protected void copyNameSpaceDecl() {
        if (recordDepth < currDepth) {
            return;
        }
        int stop = xmlSR.getNamespaceCount();
        if (stop > 0) {
            String nsDeclPrefix, nsDeclUri;
            //checking namespace declarations
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
    
    @Override
    public RecordReader<DocumentURI, Text> createRecordReader(InputSplit is,
        TaskAttemptContext tac) {

        return new AggregateXMLAdvReader(recordName, recordNamespace, nsctx);

    }

 
}
